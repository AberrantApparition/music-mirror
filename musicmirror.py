#!/usr/bin/env python3

# pylint: disable=invalid-name
# pylint: disable=line-too-long
# pylint: disable=missing-function-docstring
# pylint: disable=no-else-return
# pylint: disable=too-many-lines

"""
Maintain a mirror image of a FLAC music library, transcoded to Opus.
A fingerprint of each file is saved in `fingerprints.yaml`, allowing for incremental processing of only updated or new files.
Run `./musicmirror --help` for options
"""

import argparse
from collections import namedtuple
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import total_ordering
import hashlib
from itertools import chain
import os
from pathlib import Path
from platform import system
from re import sub, MULTILINE
from shlex import quote
import shutil
import signal
import stat
import subprocess
import sys
from threading import Lock, current_thread, local
from time import time
from typing import Tuple, Dict, Union
import yaml

CONFIG_FILE = "config.yaml"
LOG_PREFIX_INDENT = "                               "
NEWLINE = "\n"

thread_info = local()

Format = namedtuple('Format', 'HEADER OKBLUE OKCYAN OKGREEN WARNING FAIL ENDC BOLD')(
    '\033[95m',
    '\033[94m',
    '\033[96m',
    '\033[92m',
    '\033[93m',
    '\033[91m',
    '\033[0m',
    '\033[1m'
)

NoFormat = namedtuple('NoFormat', 'HEADER OKBLUE OKCYAN OKGREEN WARNING FAIL ENDC BOLD')('','','','','','','','')

@total_ordering
class ExitCode(Enum): # pylint: disable=missing-class-docstring
    OK    = 0
    WARN  = 1
    ERROR = 2
    def __lt__(self, other) -> Union[bool, type(NotImplemented) ]:
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

@total_ordering
class LogLevel(Enum): # pylint: disable=missing-class-docstring
    ERROR = 0
    WARN  = 1
    INFO  = 2
    DEBUG = 3
    TRACE = 4
    def __lt__(self, other) -> Union[bool, type(NotImplemented) ]:
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

def SetThreadName() -> None:
    thread_name = current_thread().name
    if thread_name == "MainThread":
        thread_info.name = "Main"
    else:
        thread_num = thread_name.split('_')[-1].zfill(2)
        thread_info.name = f"WT{thread_num}"

def Log(level, log, always_log=False) -> None:
    exit_early = False
    if always_log or level <= cfg["log_level"]: # pylint: disable=possibly-used-before-assignment
        timestamp = str(datetime.now()).split(" ")[1]

        if not hasattr(thread_info, 'name'):
            SetThreadName()

        match level:
            case LogLevel.ERROR:
                full_log = f"[{timestamp}][{thread_info.name}][{fmt.FAIL}{fmt.BOLD}ERROR{fmt.ENDC}] {log}"
                exit_early = True
            case LogLevel.WARN:
                full_log = f"[{timestamp}][{thread_info.name}][{fmt.WARNING}{fmt.BOLD}WARN{fmt.ENDC} ] {log}"
            case LogLevel.INFO:
                full_log = f"[{timestamp}][{thread_info.name}][{fmt.OKGREEN}INFO{fmt.ENDC} ] {log}"
            case LogLevel.DEBUG:
                full_log = f"[{timestamp}][{thread_info.name}][{fmt.OKBLUE}DEBUG{fmt.ENDC}] {log}"
            case LogLevel.TRACE:
                full_log = f"[{timestamp}][{thread_info.name}][{fmt.OKCYAN}TRACE{fmt.ENDC}] {log}"
            case _:
                full_log = f"[{timestamp}][{thread_info.name}][{fmt.FAIL}{fmt.BOLD}ERROR{fmt.ENDC}] " \
                           f"Invalid log level '{level}' for log '{log}'"
                exit_early = True

        with print_lock: # pylint: disable=possibly-used-before-assignment
            print(full_log, file=(sys.stderr if exit_early else None))

        if exit_early:
            flag.QuitWithoutSaving(ExitCode.ERROR.value) # pylint: disable=possibly-used-before-assignment

def ReadConfig(config_path) -> dict:
    Log(LogLevel.INFO, f"Reading configuration settings from {FormatPath(config_path)}", always_log=True)

    with open(config_path, encoding="utf-8") as stream:
        try:
            cfg_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            Log(LogLevel.ERROR, str(exc), always_log=True)

    Log(LogLevel.INFO, f"Configuration settings read from {config_path}", always_log=True)

    return cfg_dict

def ValidateConfigDictKey(config, key_name, expected_type) -> bool:
    if key_name in config:
        if isinstance(config[key_name], expected_type):
            return True
        else:
            Log(LogLevel.WARN, f"Config option {key_name} has unexpected type {type(config[key_name])}", always_log=True)
            return False
    else:
        Log(LogLevel.WARN, f"Config option {key_name} not found", always_log=True)
        return False

def ValidateConfig(config) -> bool:
    global fmt

    ok = True

    ok = ok and ValidateConfigDictKey(config, "log_level", str)
    ok = ok and ValidateConfigDictKey(config, "library_status_path", str)
    ok = ok and ValidateConfigDictKey(config, "library_path", str)
    ok = ok and ValidateConfigDictKey(config, "output_library_path", str)
    ok = ok and ValidateConfigDictKey(config, "library_playlist_path", str)
    ok = ok and ValidateConfigDictKey(config, "portable_playlist_path", str)
    ok = ok and ValidateConfigDictKey(config, "opus_bitrate", int)
    ok = ok and ValidateConfigDictKey(config, "allow_library_modification", bool)
    ok = ok and ValidateConfigDictKey(config, "use_hash_as_fingerprint", bool)
    ok = ok and ValidateConfigDictKey(config, "num_threads", int)
    ok = ok and ValidateConfigDictKey(config, "file_mirror_method", str)
    ok = ok and ValidateConfigDictKey(config, "log_full_paths", bool)
    ok = ok and ValidateConfigDictKey(config, "color_logs", bool)
    ok = ok and ValidateConfigDictKey(config, "ignore_hidden", bool)
    ok = ok and ValidateConfigDictKey(config, "check_padding", bool)
    ok = ok and ValidateConfigDictKey(config, "min_padding_size", int)
    ok = ok and ValidateConfigDictKey(config, "max_padding_size", int)
    ok = ok and ValidateConfigDictKey(config, "target_padding_size", int)

    if not ok:
        return False

    fmt = Format if config["color_logs"] else NoFormat

    ok = ValidateConfigPaths(config)

    match config["log_level"]:
        case "error":
            config["log_level"] = LogLevel.ERROR
        case "warn":
            config["log_level"] = LogLevel.WARN
        case "info":
            config["log_level"] = LogLevel.INFO
        case "debug":
            config["log_level"] = LogLevel.DEBUG
        case "trace":
            config["log_level"] = LogLevel.TRACE
        case _:
            Log(LogLevel.WARN, f"Invalid log level {config['log_level']}", always_log=True)
            ok = False
    if ok:
        Log(LogLevel.INFO, f"Log level set to {config['log_level']}", always_log=True)

    # Do not validate opus max bitrate here because valid range depends on the number of audio channels. Leave it up to the user to get it right
    if config["opus_bitrate"] <= 0:
        Log(LogLevel.WARN, "Opus bitrate must be a positive integer", always_log=True)
        ok = False

    if config["min_padding_size"] < 0:
        Log(LogLevel.WARN, "Min flac padding size cannot be negative", always_log=True)
        ok = False

    if config["max_padding_size"] < 0:
        Log(LogLevel.WARN, "Max flac padding size cannot be negative", always_log=True)
        ok = False

    if config["target_padding_size"] < 0:
        Log(LogLevel.WARN, "Target flac padding size cannot be negative", always_log=True)
        ok = False

    if config["min_padding_size"] > config["target_padding_size"]:
        Log(LogLevel.WARN, "min_padding_size cannot be greater than target_padding_size", always_log=True)
        ok = False

    if config["min_padding_size"] > config["max_padding_size"]:
        Log(LogLevel.WARN, "min_padding_size cannot be greater than max_padding_size", always_log=True)
        ok = False

    if config["target_padding_size"] > config["max_padding_size"]:
        Log(LogLevel.WARN, "target_padding_size cannot be greater than max_padding_size", always_log=True)
        ok = False

    if config["num_threads"] > cpu_count: # pylint: disable=possibly-used-before-assignment
        Log(LogLevel.WARN,
            f"Number of worker threads ({config['num_threads']}) cannot exceed number of cores available to process ({cpu_count})",
            always_log=True)
        ok = False

    if config["file_mirror_method"] != "copy" and \
       config["file_mirror_method"] != "soft_link" and \
       config["file_mirror_method"] != "hard_link":
        Log(LogLevel.WARN,
            f"Invalid file mirror method {config['file_mirror_method']}. Supported options are copy, soft_link, hard_link",
            always_log=True)
        ok = False

    return ok

def RestoreStdinAttr() -> None:
    # pylint: disable=possibly-used-before-assignment
    if not is_windows:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, original_stdin_attr)

class GracefulExiter():
    state: bool
    exit_code: ExitCode

    def __init__(self) -> None:
        self.state = False
        self.exit_code = ExitCode.OK
        signal.signal(signal.SIGINT, self.ChangeState)
        signal.signal(signal.SIGHUP, self.ChangeState)
        signal.signal(signal.SIGTERM, self.ChangeState)

    def ChangeState(self, signum, _frame) -> None:
        signal_log = f"\nReceived signal {signal.Signals(signum).name}; finishing processing"
        if signal.Signals(signum) == signal.SIGINT:
            signal_log += " (repeat to exit now)"
        with print_lock:
            print(signal_log)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        self.state = True

    def SetExitCode(self, new_exit_code) -> None:
        self.exit_code = max(self.exit_code, new_exit_code)

    def Exit(self) -> bool:
        return self.state

    def SaveAndQuitIfSignalled(self, exit_arg=None) -> None:
        if self.Exit():
            self.SaveAndQuit(exit_arg)

    def QuitWithoutSavingIfSignalled(self, exit_arg=None) -> None:
        if self.Exit():
            self.QuitWithoutSaving(exit_arg)

    def SaveAndQuit(self, exit_arg=None) -> None:
        WriteCache()
        self.QuitWithoutSaving(exit_arg)

    def QuitWithoutSaving(self, exit_arg=None) -> None:
        RestoreStdinAttr()
        sys.exit(exit_arg if exit_arg else self.exit_code.value)

# Leave the date off the end of the vendor string; cannot get the date without manually making a big version->date map
def ConvertFlacVersionToVendorString(version) -> str:
    version_number = version.split(' ')[1]
    return f"reference libFLAC {version_number}"

def AppendPathSeparator(path) -> str:
    if not path.endswith(os.sep):
        path += os.sep
    return path

def CheckDependencies() -> Tuple[str, str]:
    try:
        flac_output = subprocess.run(['flac', '--version'], capture_output=True, check=True)
        flac_version_str = flac_output.stdout.decode('utf-8')[:-1]
        flac_version_str = ConvertFlacVersionToVendorString(flac_version_str)
    except subprocess.CalledProcessError as exc:
        if exc.returncode < 0:
            flag.QuitWithoutSaving()
        Log(LogLevel.WARN, f"flac codec unavailable - cannot encode, decode, or test FLACs: {str(exc)}")

    try:
        metaflac_output = subprocess.run(['metaflac', '--version'], capture_output=True, check=True)
        metaflac_version = metaflac_output.stdout.decode('utf-8')[:-1]
    except subprocess.CalledProcessError as exc:
        if exc.returncode < 0:
            flag.QuitWithoutSaving()
        Log(LogLevel.WARN, f"metaflac unavailable - cannot adjust padding in FLACs: {str(exc)}")

    try:
        opus_output = subprocess.run(['opusenc', '--version'], capture_output=True, check=True)
        opus_version_str = opus_output.stdout.decode('utf-8').split("\n")[0]
    except subprocess.CalledProcessError as exc:
        if exc.returncode < 0:
            flag.QuitWithoutSaving()
        Log(LogLevel.WARN, f"opus codec unavailable - cannot encode Opus files: {str(exc)}")

    Log(LogLevel.INFO, f"Python version:   {str(sys.version)}")
    if flac_version_str:
        Log(LogLevel.INFO, f"flac version:     {flac_version_str}")
    if metaflac_version:
        Log(LogLevel.INFO, f"metaflac version: {metaflac_version}")
    if opus_version_str:
        Log(LogLevel.INFO, f"Opus version:     {opus_version_str}")

    return flac_version_str, opus_version_str

def ValidateDependencyConfigArgumentCombinations() -> None:
     # pylint: disable=possibly-used-before-assignment
    if test_specified and not flac_version:
        Log(LogLevel.ERROR, "flac codec unavailable to test FLACs with")

    if not cfg["allow_library_modification"] and args.func is reencode_library:
        Log(LogLevel.ERROR, "Config setting 'allow_library_modification' is disabled. Enable to allow reencoding of library")

    if not flac_version and args.func is reencode_library:
        Log(LogLevel.ERROR, "Cannot reencode library without a FLAC codec available")

    if not opus_version and args.func is mirror_library:
        Log(LogLevel.ERROR, "Cannot transcode portable library without an Opus encoder available")

    # Hard links require both links to be on the same filesystem
    if hasattr(args, 'hard_link') and args.hard_link and \
        os.stat(cfg["library_path"]).st_dev != os.stat(cfg["output_library_path"]).st_dev:
        Log(LogLevel.ERROR, "To use hard links the main library and portable library must reside on the same filesystem")

def ValidateConfigPaths(config) -> bool: # pylint: disable=too-many-branches
    ok = True

    config["library_status_path"] = os.path.expanduser(config["library_status_path"])
    config["library_path"] = AppendPathSeparator(os.path.expanduser(config["library_path"]))
    config["output_library_path"] = AppendPathSeparator(os.path.expanduser(config["output_library_path"]))
    config["library_playlist_path"] = AppendPathSeparator(os.path.expanduser(config["library_playlist_path"]))
    config["portable_playlist_path"] = AppendPathSeparator(os.path.expanduser(config["portable_playlist_path"]))

    config["formatted_library_status_path"] = FormatPath(config["library_status_path"])
    config["formatted_library_path"] = FormatPath(config["library_path"], fmt.OKGREEN)
    config["formatted_output_library_path"] = FormatPath(config["output_library_path"], fmt.OKBLUE)
    config["formatted_library_playlist_path"] = FormatPath(config["library_playlist_path"], fmt.OKGREEN)
    config["formatted_portable_playlist_path"] = FormatPath(config["portable_playlist_path"], fmt.OKBLUE)

    library_status_path_obj = Path(config["library_status_path"])
    library_path_obj = Path(config["library_path"])
    output_library_path_obj = Path(config["output_library_path"])
    library_playlist_path_obj = Path(config["library_playlist_path"])
    portable_playlist_path_obj = Path(config["portable_playlist_path"])

    if config["library_path"] == "" or config["output_library_path"] == "":
        Log(LogLevel.WARN,
            "Library path and output library path must be configured in config.yaml",
            always_log=True)
        ok = False

    if not library_status_path_obj.is_file():
        Log(LogLevel.WARN,
            f"Library status path {config['formatted_library_status_path']} does not exist or is not a file",
            always_log=True)
        ok = False
    if not library_path_obj.is_dir():
        Log(LogLevel.WARN,
            f"Library path {config['formatted_library_path']} does not exist or is not a directory",
            always_log=True)
        ok = False

    if config["output_library_path"] == config["library_path"]:
        Log(LogLevel.WARN,
            f"Output library path {config['formatted_output_library_path']} matches library path {config['formatted_library_path']}",
            always_log=True)
        ok = False
    if library_path_obj in output_library_path_obj.parents:
        Log(LogLevel.WARN,
            f"Output library path {config['formatted_output_library_path']} is inside library path {config['formatted_library_path']}",
            always_log=True)
        ok = False
    if output_library_path_obj in library_path_obj.parents:
        Log(LogLevel.WARN,
            f"Library path {config['formatted_library_path']} is inside output library path {config['formatted_output_library_path']}",
            always_log=True)
        ok = False

    if args.func is convert_playlists:
        if not library_playlist_path_obj.is_dir():
            Log(LogLevel.WARN,
                f"Library playlist path {config['formatted_library_playlist_path']} does not exist or is not a directory",
                always_log=True)
            ok = False
        if not portable_playlist_path_obj.is_dir():
            Log(LogLevel.WARN,
                f"Portable playlist path {config['formatted_portable_playlist_path']} does not exist or is not a directory",
                always_log=True)
            ok = False

        if config["portable_playlist_path"] == config["library_playlist_path"]:
            Log(LogLevel.WARN,
                f"Portable playlists path {config['formatted_portable_playlist_path']} matches library playlist path {config['formatted_library_playlist_path']}",
                always_log=True)
            ok = False
        if library_playlist_path_obj in portable_playlist_path_obj.parents:
            Log(LogLevel.WARN,
                f"Portable playlists path {config['formatted_portable_playlist_path']} is inside library playlist path {config['formatted_library_playlist_path']}",
                always_log=True)
            ok = False
        if portable_playlist_path_obj in library_playlist_path_obj.parents:
            Log(LogLevel.WARN,
                f"Library playlists path {config['formatted_library_playlist_path']} is inside portable playlist path {config['formatted_portable_playlist_path']}",
                always_log=True)
            ok = False

        if config["portable_playlist_path"] == config["output_library_path"]:
            Log(LogLevel.WARN,
                f"Portable playlists path {config['formatted_portable_playlist_path']} matches output library playlist path {config['formatted_output_library_path']}",
                always_log=True)
            ok = False
        if output_library_path_obj in portable_playlist_path_obj.parents:
            Log(LogLevel.WARN,
                f"Portable playlists path {config['formatted_portable_playlist_path']} is inside output library path {config['formatted_output_library_path']}",
                always_log=True)
            ok = False

    return ok

@dataclass
class DirEntry(): # pylint: disable=too-many-instance-attributes
    path: str
    present_in_last_scan: bool
    mirrored: bool

    # Not read from or saved to cache
    library_path: str
    portable_path: str
    formatted_path: str
    formatted_portable_path: str
    present_in_current_scan: bool

    def __init__(self, saved_entry=None, full_path=None, rel_path=None) -> None:
        if saved_entry is not None:
            # Entry created from cache
            self.path = AppendPathSeparator(saved_entry[0])
            self.library_path = os.path.join(cfg["library_path"], self.path)
            for key, value in saved_entry[1].items():
                setattr(self, key, value)
            self.present_in_current_scan = False
        elif full_path is not None and rel_path is not None:
            # Entry created from scan
            self.path = rel_path
            self.library_path = full_path
            self.present_in_last_scan = True
            self.present_in_current_scan = True
            self.mirrored = False
        else:
            Log(LogLevel.ERROR, "SHOULD NOT HAPPEN: bad dir init arguments")

        if cfg["log_full_paths"]:
            self.formatted_path = FormatPath(self.library_path, fmt.OKGREEN)
        else:
            self.formatted_path = FormatPath(self.path, fmt.OKGREEN)

        if args.func is mirror_library:
            self.portable_path = os.path.join(cfg["output_library_path"], self.path)
            if cfg["log_full_paths"]:
                self.formatted_portable_path = FormatPath(self.portable_path, fmt.OKBLUE)
            else:
                self.formatted_portable_path = FormatPath(self.path, fmt.OKBLUE)

    def asdict(self) -> Dict:
        return \
        {
            self.path: \
            {
                'mirrored': self.mirrored,
                'present_in_last_scan': self.present_in_last_scan
            }
        }

@dataclass
class FileEntry(): # pylint: disable=too-many-instance-attributes
    path: str
    fingerprint_on_last_scan: str
    fingerprint_on_last_mirror: str
    present_in_last_scan: bool

    # Not read from or saved to cache
    library_path: str
    portable_path: str
    formatted_path: str
    formatted_portable_path: str
    present_in_current_scan: bool

    def __init__(self, saved_entry=None, full_path=None, rel_path=None, fingerprint=None) -> None:
        if saved_entry is not None:
            # Entry created from cache
            self.path = saved_entry[0]
            self.library_path = os.path.join(cfg["library_path"], self.path)
            for key, value in saved_entry[1].items():
                setattr(self, key, value)
            self.present_in_current_scan = False
        elif full_path is not None and rel_path is not None and fingerprint is not None:
            # Entry created from scan
            self.path = rel_path
            self.library_path = full_path
            self.fingerprint_on_last_scan = fingerprint
            self.fingerprint_on_last_mirror = ''
            self.present_in_last_scan = True
            self.present_in_current_scan = True
        else:
            Log(LogLevel.ERROR, "SHOULD NOT HAPPEN: bad file init arguments")

        if cfg["log_full_paths"]:
            self.formatted_path = FormatPath(self.library_path, fmt.OKGREEN)
        else:
            self.formatted_path = FormatPath(self.path, fmt.OKGREEN)

        if args.func is mirror_library:
            self.portable_path = os.path.join(cfg["output_library_path"], self.path)
            if cfg["log_full_paths"]:
                self.formatted_portable_path = FormatPath(self.portable_path, fmt.OKBLUE)
            else:
                self.formatted_portable_path = FormatPath(self.path, fmt.OKBLUE)

    def asdict(self) -> Dict:
        return \
        {
            self.path: \
            {
                'fingerprint_on_last_mirror': self.fingerprint_on_last_mirror,
                'fingerprint_on_last_scan': self.fingerprint_on_last_scan,
                'present_in_last_scan': self.present_in_last_scan
            }
        }

@dataclass
class FlacEntry(): # pylint: disable=too-many-instance-attributes
    path: str
    fingerprint_on_last_scan: str
    present_in_last_scan: bool
    fingerprint_on_last_reencode: str
    fingerprint_on_last_repad: str
    fingerprint_on_last_transcode: str
    fingerprint_on_last_test: str
    reencode_ignore_last_fingerprint_change: bool
    transcode_ignore_last_fingerprint_change: bool
    test_pass: bool
    flac_codec_on_last_test: str
    flac_codec_on_last_reencode: str
    opus_codec_on_last_transcode: str

    # Not read from or saved to cache
    library_path: str
    portable_path: str
    quoted_path: str
    formatted_path: str
    formatted_portable_path: str
    present_in_current_scan: bool

    def __init__(self, saved_entry=None, full_path=None, rel_path=None, fingerprint=None) -> None:
        if saved_entry is not None:
            # Entry created from cache
            self.path = saved_entry[0]
            self.library_path = os.path.join(cfg["library_path"], self.path)

            # Add new fields in case user has a fingerprints.yaml without updated fields
            saved_entry[1].setdefault("fingerprint_on_last_repad", '')
            saved_entry[1].setdefault("reencode_ignore_last_fingerprint_change", False)
            saved_entry[1].setdefault("transcode_ignore_last_fingerprint_change", False)

            for key, value in saved_entry[1].items():
                setattr(self, key, value)
            self.present_in_current_scan = False
        elif full_path is not None and rel_path is not None and fingerprint is not None:
            # Entry created from scan
            self.path = rel_path
            self.library_path = full_path
            self.fingerprint_on_last_scan = fingerprint
            self.fingerprint_on_last_reencode = ''
            self.fingerprint_on_last_repad = ''
            self.fingerprint_on_last_transcode = ''
            self.fingerprint_on_last_test = ''
            self.reencode_ignore_last_fingerprint_change = False
            self.transcode_ignore_last_fingerprint_change = False
            self.flac_codec_on_last_test = ''
            self.flac_codec_on_last_reencode = ''
            self.opus_codec_on_last_transcode = ''
            self.test_pass = False
            self.present_in_last_scan = True
            self.present_in_current_scan = True
        else:
            Log(LogLevel.ERROR, "SHOULD NOT HAPPEN: bad flac init arguments")

        self.quoted_path = quote(self.library_path)

        if cfg["log_full_paths"]:
            self.formatted_path = AddColor(self.quoted_path, fmt.OKGREEN)
        else:
            self.formatted_path = FormatPath(self.path, fmt.OKGREEN)

        if args.func is mirror_library:
            relative_portable_path = self.path[:-5] + ".opus"
            self.portable_path = os.path.join(cfg["output_library_path"], relative_portable_path)
            if cfg["log_full_paths"]:
                self.formatted_portable_path = FormatPath(self.portable_path, fmt.OKBLUE)
            else:
                self.formatted_portable_path = FormatPath(relative_portable_path, fmt.OKBLUE)

    def asdict(self) -> Dict:
        return \
        {
            self.path: \
            {
                'fingerprint_on_last_reencode': self.fingerprint_on_last_reencode,
                'fingerprint_on_last_repad': self.fingerprint_on_last_repad,
                'fingerprint_on_last_scan': self.fingerprint_on_last_scan,
                'fingerprint_on_last_test': self.fingerprint_on_last_test,
                'fingerprint_on_last_transcode': self.fingerprint_on_last_transcode,
                'flac_codec_on_last_reencode': self.flac_codec_on_last_reencode,
                'flac_codec_on_last_test': self.flac_codec_on_last_test,
                'opus_codec_on_last_transcode': self.opus_codec_on_last_transcode,
                'present_in_last_scan': self.present_in_last_scan,
                'reencode_ignore_last_fingerprint_change': self.reencode_ignore_last_fingerprint_change,
                'test_pass': self.test_pass,
                'transcode_ignore_last_fingerprint_change': self.transcode_ignore_last_fingerprint_change
            }
        }

@dataclass
class Cache():
    path: str
    dirs: list[DirEntry]
    files: list[FileEntry]
    flacs: list[FlacEntry]

    def __init__(self, d=None) -> None:
        if d is not None:
            self.path = d[0]
            for key, value in d[1].items():
                setattr(self, key, value)

        else:
            self.path = ''
            self.dirs = []
            self.files = []
            self.flacs = []

    def asdict(self) -> Dict:
        cache_dict = {}
        cache_dict["dirs"] = {}
        cache_dict["files"] = {}
        cache_dict["flacs"] = {}

        for entry in self.dirs:
            cache_dict["dirs"].update(entry.asdict())
        for entry in self.files:
            cache_dict["files"].update(entry.asdict())
        for entry in self.flacs:
            cache_dict["flacs"].update(entry.asdict())

        return cache_dict

def TimeCommand(start_time, command_desc, time_log_level) -> None:
    end_time = time()
    command_dur = end_time - start_time
    Log(time_log_level, f"{command_desc} took {command_dur:.6f} seconds to run")

def PrintFailureList(description, fail_list) -> None:
    for item in fail_list:
        description = f"{description}\n{item}"
    Log(LogLevel.WARN, f"{fmt.WARNING}{description}{fmt.ENDC}")

def ReadCache() -> None:
    global cache

    start_time = time()
    cache = Cache()

    Log(LogLevel.INFO, f"Reading library status from {cfg['formatted_library_status_path']}")

    with open(cfg["library_status_path"], encoding="utf-8") as stream:
        try:
            cache_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            Log(LogLevel.ERROR, str(exc))

    if cache_dict:
        if cache_dict["dirs"]:
            for entry in cache_dict["dirs"].items():
                cache.dirs.append(DirEntry(saved_entry=entry))
        if cache_dict["files"]:
            for entry in cache_dict["files"].items():
                cache.files.append(FileEntry(saved_entry=entry))
        if cache_dict["flacs"]:
            for entry in cache_dict["flacs"].items():
                cache.flacs.append(FlacEntry(saved_entry=entry))

    TimeCommand(start_time, "Reading library status", LogLevel.INFO)

    flag.QuitWithoutSavingIfSignalled() # pylint: disable=possibly-used-before-assignment

def WriteCache() -> None:
    start_time = time()

    Log(LogLevel.INFO, f"Saving library status to {cfg['formatted_library_status_path']}")
    with open(cfg["library_status_path"], "w", encoding="utf-8") as stream:
        try:
            stream.write(yaml.dump(cache.asdict()))
        except yaml.YAMLError as exc:
            Log(LogLevel.ERROR, str(exc))

    TimeCommand(start_time, "Writing library status", LogLevel.INFO)

def FormatPath(path, color='') -> str:
    color_reset = fmt.ENDC if color else ''
    return f'{color}{quote(path)}{color_reset}'

def AddColor(text, color='') -> str:
    color_reset = fmt.ENDC if color else ''
    return f'{color}{text}{color_reset}'

def DetectPlaylist(file_path) -> bool:
    file_extension = file_path.split(".")[-1]
    return file_extension in ('m3u', 'm3u8')

def ConvertPlaylist(file_path, output_path, playlist_convert_str) -> bool:
    try:
        for encoding in ('utf-8', 'cp1252', 'latin1'):
            try:
                with open(file_path, 'r', encoding=encoding) as in_playlist:
                    data = in_playlist.read()
            except UnicodeDecodeError:
                Log(LogLevel.TRACE, f"Could not decode playlist {playlist_convert_str} with encoding {encoding}")
                continue
            updated_string = sub(r".flac$", ".opus", data, flags=MULTILINE)
            output_file = Path(output_path)
            output_file.parent.mkdir(exist_ok=True, parents=True)
            output_file.write_text(updated_string, encoding=encoding)
            return True
        Log(LogLevel.WARN, f"Could not decode playlist {playlist_convert_str}")
    except OSError as exc:
        Log(LogLevel.WARN, f"Error when converting playlist {playlist_convert_str}: {exc}")
    return False

def TestFlac(file_path) -> Tuple[bool, str]:
    with subprocess.Popen(['flac', '-t', '-w', '-s', file_path],
                          text=True,
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.PIPE,
                          start_new_session=True) as p: # New process group to ignore signals
        try:
            errs = p.communicate(timeout=60)[1]
            if p.returncode == 0:
                return True, ""
            else:
                if p.returncode < 0:
                    status = f"{fmt.WARNING}FLAC test terminated by signal {-1 * p.returncode}{fmt.ENDC}"
                else:
                    test_error = errs.removesuffix('\n').split(".flac: ")[-1][:-1]
                    status = f"{fmt.WARNING}FLAC test failed:\n{test_error}{fmt.ENDC}"
                return False, status
        except subprocess.TimeoutExpired:
            status = "FLAC test subprocess timed out"
            return False, status

def ConditionallyRunFlacTest(entry, fingerprint) -> Tuple[bool, bool, str]:
    # pylint: disable=possibly-used-before-assignment
    test_ran = False
    status = ""
    test_due_to_change = test_specified and fingerprint != entry.fingerprint_on_last_test
    retest_due_to_update = retest_on_update and entry.flac_codec_on_last_test != flac_version

    if test_due_to_change or retest_due_to_update or test_force:
        entry.test_pass, status = TestFlac(entry.library_path)
        entry.fingerprint_on_last_test = fingerprint
        entry.flac_codec_on_last_test = flac_version
        test_ran = True

    return test_ran, entry.test_pass, status

def CalculateFileHash(file_path) -> str:
    if sys.version_info >= (3, 11):
        with open(file_path, "rb") as f:
            return hashlib.file_digest(f, 'sha224').hexdigest()
    else:
        h = hashlib.sha224(usedforsecurity=False)
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
        return h.hexdigest()

def CalculateFingerprint(file_path) -> str:
    if cfg["use_hash_as_fingerprint"]:
        return CalculateFileHash(file_path)
    else:
        return str(datetime.fromtimestamp(Path(file_path).stat().st_mtime, timezone.utc))

def ReencodeFlac(entry) -> bool:
    """Return whether successful"""
    reencode_log = f"Reencode {entry.formatted_path}"

    if args.dry_run:
        Log(LogLevel.TRACE, f"Dry run: {reencode_log}")
        return True

    # Write to a temp file first, then overwrite if encoding successful
    tmp_path = entry.library_path + ".tmp"
    flac_args = ['flac',
                 '--silent',
                 '--best',
                 '--verify',
                 f'--padding={cfg["target_padding_size"]}',
                 '--no-preserve-modtime',
                 entry.library_path,
                 '-o',
                 tmp_path]

    with subprocess.Popen(flac_args,
                          text=True,
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.PIPE,
                          start_new_session=True) as p: # New process group to ignore signals
        try:
            errs = p.communicate(timeout=60)[1]
            if p.returncode == 0:
                shutil.move(tmp_path, entry.library_path)

                fingerprint = CalculateFingerprint(entry.library_path)
                # Do not trigger an unnecessary transcode in a future run due to this reencode
                if entry.fingerprint_on_last_scan == entry.fingerprint_on_last_transcode:
                    entry.transcode_ignore_last_fingerprint_change = True
                entry.fingerprint_on_last_scan = fingerprint
                entry.fingerprint_on_last_test = fingerprint
                entry.fingerprint_on_last_reencode = fingerprint
                entry.fingerprint_on_last_repad = fingerprint
                entry.flac_codec_on_last_reencode = flac_version

                if cfg["log_level"] >= LogLevel.TRACE:
                    reencode_log += f"\n{LOG_PREFIX_INDENT}    New fingerprint: {fingerprint}"
                if errs:
                    reencode_log_level = LogLevel.WARN
                    reencode_log += f"\n{fmt.WARNING}FLAC reencode passed with warnings:" \
                                    f"\n{errs.removesuffix(NEWLINE)}{fmt.ENDC}"
                else:
                    reencode_log_level = LogLevel.DEBUG
                Log(reencode_log_level, reencode_log)
                return True

            else:
                Path.unlink(Path(tmp_path), missing_ok=True)
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{reencode_log}\n" \
                                       f"{fmt.WARNING}FLAC reencode terminated by signal {-1 * p.returncode}{fmt.ENDC}")
                    return False
                else:
                    Log(LogLevel.WARN, f"{reencode_log}\n" \
                                       f"{fmt.WARNING}FLAC reencode failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix(NEWLINE)}{fmt.ENDC}")
                    return False

        except subprocess.TimeoutExpired:
            Path.unlink(Path(tmp_path), missing_ok=True)
            Log(LogLevel.WARN, f"Reencode subprocess for {reencode_log} timed out")
            return False

class RepadAction(Enum): # pylint: disable=missing-class-docstring
    NONE           = 0
    MERGE_AND_SORT = 1
    RESIZE         = 2

def CheckIfRepadNecessary(entry) -> Tuple[bool, RepadAction]:
    """Return whether flac repad successful and the repad action to take. If not successful, action is always NONE"""
    repad_check_log = f"Padding check for {entry.formatted_path}"

    metaflac_args = ['metaflac', '--list', '--block-type=PADDING', entry.library_path]

    with subprocess.Popen(metaflac_args,
                          text=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          start_new_session=True) as p: # New process group to ignore signals
        try:
            outs, errs = p.communicate(timeout=60)
            if p.returncode == 0:
                outlines = outs.splitlines()

                padding_block_length_lines = [line for line in outlines if line.startswith('  length:')]
                num_padding_blocks = len(padding_block_length_lines)
                num_padding_bytes = 0
                for line in padding_block_length_lines:
                    num_padding_bytes += int(line.split(': ')[-1])

                padding_over_limit = num_padding_bytes > cfg["max_padding_size"]
                padding_under_limit = num_padding_bytes < cfg["min_padding_size"]
                if padding_over_limit or padding_under_limit:
                    Log(LogLevel.DEBUG, f"{repad_check_log}\n" \
                                        f"{LOG_PREFIX_INDENT}flac has {num_padding_bytes} bytes of padding"
                                        f"{(' (too much)' if padding_over_limit else ' (not enough)')}")
                    return True, RepadAction.RESIZE
                elif num_padding_blocks > 1:
                    Log(LogLevel.DEBUG, f"{repad_check_log}\n" \
                                        f"{LOG_PREFIX_INDENT}flac has {num_padding_blocks} padding blocks")
                    return True, RepadAction.MERGE_AND_SORT
                elif not '  is last: true' in outlines:
                    Log(LogLevel.DEBUG, f"{repad_check_log}\n" \
                                        f"{LOG_PREFIX_INDENT}flac padding is not last block")
                    return True, RepadAction.MERGE_AND_SORT
                else:
                    Log(LogLevel.TRACE, f"{repad_check_log}\n" \
                                        f"{LOG_PREFIX_INDENT}flac padding is acceptable ({num_padding_bytes} bytes)")
                    return True, RepadAction.NONE
            else:
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{repad_check_log}\n" \
                                       f"{fmt.WARNING}metaflac padding check terminated by signal {-1 * p.returncode}{fmt.ENDC}")
                else:
                    Log(LogLevel.WARN, f"{repad_check_log}\n" \
                                       f"{fmt.WARNING}metaflac padding check failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix(NEWLINE)}{fmt.ENDC}")
                return False, RepadAction.NONE

        except subprocess.TimeoutExpired:
            Log(LogLevel.WARN, f"metaflac padding check subprocess for {repad_check_log} timed out")
            return False, RepadAction.NONE

def RepadFlac(entry) -> Tuple[bool, bool]:
    """Return whether flac repad attempted and whether successful (either repad check or repad itself can fail)"""
    repad_check_ok, repad_action = CheckIfRepadNecessary(entry)

    use_shell = False

    match repad_action:
        case RepadAction.MERGE_AND_SORT:
            metaflac_args = ['metaflac', '--sort-padding', entry.library_path]
            repad_description = "sort and merge padding"
        case RepadAction.RESIZE:
            # metaflac does not allow --remove and --add-padding in the same command
            metaflac_args = f"metaflac --remove --block-type=PADDING --dont-use-padding {entry.quoted_path}" \
                            f"&&" \
                            f"metaflac --add-padding={cfg['max_padding_size']} {entry.quoted_path}"
            repad_description = "resize padding"
            use_shell = True
        case RepadAction.NONE:
            if repad_check_ok and not args.dry_run:
                entry.fingerprint_on_last_repad = entry.fingerprint_on_last_scan
            return False, repad_check_ok

    repad_log = f"Repad {entry.formatted_path} ({repad_description})"

    if args.dry_run:
        Log(LogLevel.TRACE, f"Dry run: {repad_log}")
        return True, True

    with subprocess.Popen(metaflac_args,
                          shell=use_shell,
                          text=True,
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.PIPE,
                          start_new_session=True) as p: # New process group to ignore signals
        try:
            errs = p.communicate(timeout=60)[1]
            if p.returncode == 0:
                fingerprint = CalculateFingerprint(entry.library_path)
                # Do not trigger an unnecessary reencode or transcode in a future run due to this repad
                if entry.fingerprint_on_last_scan == entry.fingerprint_on_last_reencode:
                    entry.reencode_ignore_last_fingerprint_change = True
                if entry.fingerprint_on_last_scan == entry.fingerprint_on_last_transcode:
                    entry.transcode_ignore_last_fingerprint_change = True
                entry.fingerprint_on_last_scan = fingerprint
                entry.fingerprint_on_last_test = fingerprint
                entry.fingerprint_on_last_repad = fingerprint

                if cfg["log_level"] >= LogLevel.TRACE:
                    repad_log += f"\n{LOG_PREFIX_INDENT}    New fingerprint: {fingerprint}"
                if errs:
                    repad_log_level = LogLevel.WARN
                    repad_log += f"\n{fmt.WARNING}FLAC repad passed with warnings:" \
                                 f"\n{errs.removesuffix(NEWLINE)}{fmt.ENDC}"
                else:
                    repad_log_level = LogLevel.DEBUG
                Log(repad_log_level, repad_log)
                return True, not errs

            else:
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{repad_log}\n" \
                                       f"{fmt.WARNING}FLAC repad terminated by signal {-1 * p.returncode}{fmt.ENDC}")
                    return True, False
                else:
                    Log(LogLevel.WARN, f"{repad_log}\n" \
                                       f"{fmt.WARNING}FLAC repad failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix(NEWLINE)}{fmt.ENDC}")
                    return True, False

        except subprocess.TimeoutExpired:
            Log(LogLevel.WARN, f"Repad subprocess for {repad_log} timed out")
            return True, False

def TranscodeFlac(entry) -> bool:
    """Return whether flac->opus transcode successful"""
    transcode_log = f"{entry.formatted_path} -> {entry.formatted_portable_path}"

    if args.dry_run:
        Log(LogLevel.TRACE, f"Dry run: {transcode_log}")
        return True, False

    transcode_args = ['opusenc',
                      '--quiet',
                      '--music',
                      '--bitrate',
                      str(cfg["opus_bitrate"]),
                      entry.library_path,
                      entry.portable_path]

    with subprocess.Popen(transcode_args,
                          text=True,
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.PIPE,
                          start_new_session=True) as p: # New process group to ignore signals
        try:
            errs = p.communicate(timeout=60)[1]
            if p.returncode == 0:

                entry.fingerprint_on_last_transcode = entry.fingerprint_on_last_scan
                entry.opus_codec_on_last_transcode = opus_version

                if errs:
                    transcode_log_level = LogLevel.WARN
                    transcode_log += f"\n{fmt.WARNING}Transcode passed with warnings:" \
                                     f"\n{errs.removesuffix(NEWLINE)}{fmt.ENDC}"
                else:
                    transcode_log_level = LogLevel.DEBUG
                Log(transcode_log_level, transcode_log)
                return True

            else:
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{transcode_log}\n" \
                                       f"{fmt.WARNING}Transcode terminated by signal {-1 * p.returncode}{fmt.ENDC}")
                    return False
                else:
                    Log(LogLevel.WARN, f"{transcode_log}\n" \
                                       f"{fmt.WARNING}Transcode failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix(NEWLINE)}{fmt.ENDC}")
                    return False

        except subprocess.TimeoutExpired:
            Log(LogLevel.WARN, f"Transcode subprocess for {transcode_log} timed out")
            return False

def CreateOrUpdateCacheDirEntry(full_path) -> int:
    relative_path = AppendPathSeparator(full_path[len(cfg["library_path"]):])

    is_new_entry = True
    for entry in cache.dirs:
        if entry.path == relative_path:
            entry_status = "Unchanged"
            entry_log_level = LogLevel.TRACE
            entry.present_in_current_scan = True
            entry.present_in_last_scan = True
            is_new_entry = False
            break

    if is_new_entry:
        entry_status = "New"
        entry_log_level = LogLevel.DEBUG
        entry = DirEntry(full_path=full_path, rel_path=relative_path)
        cache.dirs.append(entry)

    Log(entry_log_level, f"{entry.formatted_path} ({entry_status})")

    return 1 if is_new_entry else 0

def CreateOrUpdateCacheFileEntry(full_path) -> int:
    fingerprint = CalculateFingerprint(full_path)
    relative_path = full_path[len(cfg["library_path"]):]

    is_new_entry = True
    for entry in cache.files:
        if entry.path == relative_path:
            if fingerprint != entry.fingerprint_on_last_scan:
                entry.fingerprint_on_last_scan = fingerprint
                entry_status = "Modified"
                entry_log_level = LogLevel.DEBUG
            else:
                entry_status = "Unchanged"
                entry_log_level = LogLevel.TRACE
            is_new_entry = False
            entry.present_in_current_scan = True
            entry.present_in_last_scan = True
            break

    if is_new_entry:
        entry_status = "New"
        entry_log_level = LogLevel.DEBUG
        entry = FileEntry(full_path=full_path, rel_path=relative_path, fingerprint=fingerprint)
        cache.files.append(entry)

    entry_log = f"{entry.formatted_path} ({entry_status})"
    if cfg["log_level"] >= LogLevel.TRACE:
        entry_log += f"\n{LOG_PREFIX_INDENT}    Fingerprint: {fingerprint}"
    Log(entry_log_level, entry_log)

    return 1 if is_new_entry else 0

def CreateOrUpdateCacheFlacEntry(full_path) -> Tuple[bool, bool, bool]:
    """Return whether a new cache flac entry was created, whether a FLAC test was ran, and whether the test passed"""
    fingerprint = CalculateFingerprint(full_path)
    relative_path = full_path[len(cfg["library_path"]):]

    is_new_entry = True
    for entry in cache.flacs:
        if entry.path == relative_path:
            if fingerprint != entry.fingerprint_on_last_scan:
                entry.fingerprint_on_last_scan = fingerprint
                entry.reencode_ignore_last_fingerprint_change = False
                entry.transcode_ignore_last_fingerprint_change = False
                entry_status = "Modified"
                entry_log_level = LogLevel.DEBUG
            else:
                entry_status = "Unchanged"
                entry_log_level = LogLevel.TRACE
            entry.present_in_current_scan = True
            entry.present_in_last_scan = True
            is_new_entry = False
            break

    if is_new_entry:
        entry_status = "New"
        entry_log_level = LogLevel.DEBUG
        entry = FlacEntry(full_path=full_path, rel_path=relative_path, fingerprint=fingerprint)
        cache.flacs.append(entry)

    test_ran, test_pass, status = ConditionallyRunFlacTest(entry, fingerprint)

    if test_ran and not test_pass:
        entry_log_level = LogLevel.WARN

    entry_log = f"{entry.formatted_path} ({entry_status})"
    if cfg["log_level"] >= LogLevel.TRACE:
        entry_log += f"\n{LOG_PREFIX_INDENT}    Fingerprint: {fingerprint}"
    if test_ran and status:
        entry_log += f"\n{LOG_PREFIX_INDENT}    {status}"
    Log(entry_log_level, entry_log)

    return is_new_entry, test_ran, test_pass

def PrintScanSummary(stats, early_exit=False) -> None:
    if test_specified:
        summary_log_level = LogLevel.WARN if stats["num_tests_failed"] > 0 else LogLevel.INFO
        num_tests = stats["num_tests_passed"] + stats["num_tests_failed"]
        pass_fail = f": {stats['num_tests_passed']} passes, {stats['num_tests_failed']} failures" if num_tests else ""
        test_color = fmt.WARNING if stats["num_tests_failed"] else fmt.ENDC
        test_summary = f"\n{test_color}{num_tests} flac tests performed{pass_fail}{fmt.ENDC}"
    else:
        summary_log_level = LogLevel.INFO
        test_summary = ""

    scan_result = "Scan interrupted:" if early_exit else "Scan complete:"

    Log(summary_log_level, f"{scan_result}\n" \
                           f"{stats['num_dirs']} dirs ({stats['num_new_dirs']} new)\n" \
                           f"{stats['num_files']} files ({stats['num_new_files']} new)\n" \
                           f"{stats['num_flacs']} flacs ({stats['num_new_flacs']} new)" + \
                           test_summary)

    if stats["failed_flac_tests"]:
        PrintFailureList('Failed flac tests:', stats["failed_flac_tests"])
        flag.SetExitCode(ExitCode.WARN)

    if early_exit:
        flag.SaveAndQuit()

def IsHiddenFile(path) -> bool:
    if is_windows: # pylint: disable=possibly-used-before-assignment
        return bool(os.stat(path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN)
    else:
        return os.path.basename(path).startswith(".")

def IsHiddenFileOrPath(full_path) -> bool:
    # Treat an unhidden file inside a hidden directory as hidden also. Loop over all parent directories
    path = os.path.abspath(full_path)
    while path and path != "/":
        if IsHiddenFile(path):
            return True
        path = os.path.dirname(path)
    return False

def ScanLibrary() -> None:
    start_time = time()

    scan_test_log = "Scanning and testing library" if test_specified else "Scanning library"
    Log(LogLevel.INFO, f"{scan_test_log} in {cfg['formatted_library_path']}")

    stats = {
        "num_dirs": 0,
        "num_files": 0,
        "num_flacs": 0,
        "num_new_dirs": 0,
        "num_new_files": 0,
        "num_new_flacs": 0,
        "num_tests_passed": 0,
        "num_tests_failed": 0,
        "failed_flac_tests": []
    }

    flac_paths, non_flac_paths = [], []

    for root, dirs, files in os.walk(cfg["library_path"]):
        for directory in dirs:
            full_path = os.path.join(root, directory)
            if not (cfg["ignore_hidden"] and IsHiddenFileOrPath(full_path)):
                stats["num_new_dirs"] += CreateOrUpdateCacheDirEntry(full_path)
                stats["num_dirs"] += 1
            if flag.Exit():
                PrintScanSummary(stats, early_exit=True)

        for file in files:
            full_path = os.path.join(root, file)
            if not (cfg["ignore_hidden"] and IsHiddenFileOrPath(full_path)):
                if file.split(".")[-1] == "flac":
                    flac_paths.append(full_path)
                else:
                    non_flac_paths.append(full_path)
            if flag.Exit():
                PrintScanSummary(stats, early_exit=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        future_to_path = {executor.submit(CreateOrUpdateCacheFileEntry, full_path): full_path for full_path in non_flac_paths}
        for future in concurrent.futures.as_completed(future_to_path):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                break
        for future in future_to_path:
            if not future.cancelled():
                stats["num_new_files"] += future.result()
                stats["num_files"] += 1

    if flag.Exit():
        PrintScanSummary(stats, early_exit=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        future_to_path = {executor.submit(CreateOrUpdateCacheFlacEntry, full_path): full_path for full_path in flac_paths}
        for future in concurrent.futures.as_completed(future_to_path):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                break
        for future in future_to_path:
            if not future.cancelled():
                is_new, test_ran, test_pass = future.result()
                if test_ran and test_pass:
                    stats["num_tests_passed"] += 1
                elif test_ran and not test_pass:
                    stats["num_tests_failed"] += 1
                    stats["failed_flac_tests"].append(future_to_path[future])
                stats["num_new_flacs"] += 1 if is_new else 0
                stats["num_flacs"] += 1

    PrintScanSummary(stats, flag.Exit())

    TimeCommand(start_time, scan_test_log, LogLevel.INFO)

def CheckForOrphanedCache() -> None:
    start_time = time()

    stats = {
        "num_orphaned_dirs": 0,
        "num_orphaned_files": 0,
        "num_orphaned_flacs": 0
    }

    first_file_index = len(cache.dirs)
    first_flac_index = len(cache.dirs) + len(cache.files)

    for index, entry in enumerate(chain(cache.dirs, cache.files, cache.flacs)):
        if not entry.present_in_current_scan:
            Log(LogLevel.WARN, f"Status entry not found in last scan: {entry.formatted_path}")
            entry.present_in_last_scan = False
            if index < first_file_index:
                stats['num_orphaned_dirs'] += 1
            elif index < first_flac_index:
                stats['num_orphaned_files'] += 1
            else:
                stats['num_orphaned_flacs'] += 1

    if stats['num_orphaned_dirs'] or stats['num_orphaned_files'] or stats['num_orphaned_flacs']:
        Log(LogLevel.INFO, f"Found orphaned status entries:\n" \
                           f"{stats['num_orphaned_dirs']} dirs\n" \
                           f"{stats['num_orphaned_files']} files\n" \
                           f"{stats['num_orphaned_flacs']} flacs")
        if args.func is mirror_library:
            Log(LogLevel.INFO, "Orphaned entries and the mirrored files they point to will be removed in the upcoming mirror")
        else:
            Log(LogLevel.INFO, "Orphaned entries and the mirrored files they point to will be removed in the next mirror command")

    TimeCommand(start_time, "Checking for orphaned status entries", LogLevel.INFO)

def PrintReencodeSummary(stats, early_exit) -> None:
    reencode_result = "Library reencode interrupted:" if early_exit else "Library reencode complete:"
    if stats['num_failed'] > 0:
        summary_log_level = LogLevel.WARN
        reencode_fail = f"\n{fmt.WARNING}{stats['num_failed']} not reencoded due to errors{fmt.ENDC}"
        flag.SetExitCode(ExitCode.WARN)
    else:
        summary_log_level = LogLevel.INFO
        reencode_fail = ""
    if early_exit:
        interrupted_summary = f"\n{stats['num_interrupted']} interrupted"
    else:
        interrupted_summary = ""
    Log(summary_log_level, f"{reencode_result}\n" \
                           f"{stats['num_total']} total FLACs\n" \
                           f"{stats['num_reencoded']} reencoded" + \
                           reencode_fail + \
                           interrupted_summary)

    if stats['failed_reencodes']:
        PrintFailureList('Failed reencodes:', stats['failed_reencodes'])

    if early_exit:
        flag.SaveAndQuit()

def ReencodeLibrary() -> None:
    start_time = time()

    Log(LogLevel.INFO, f"Reencoding FLACs in {cfg['formatted_library_path']}")

    stats = {
        "num_reencoded": 0,
        "num_failed": 0,
        "num_interrupted": 0,
        "num_total": 0,
        "failed_reencodes": []
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        future_to_entry = {}
        for entry in cache.flacs:
            if entry.present_in_last_scan:
                stats['num_total'] += 1
                reencode_for_change = args.reencode_on_change and \
                                      entry.fingerprint_on_last_scan != entry.fingerprint_on_last_reencode and \
                                      not entry.reencode_ignore_last_fingerprint_change
                reencode_for_update = args.reencode_on_update and entry.flac_codec_on_last_reencode != flac_version
                if args.force or \
                   not entry.fingerprint_on_last_reencode or \
                   reencode_for_change or \
                   reencode_for_update:
                    future_to_entry[executor.submit(ReencodeFlac, entry)] = entry
        for future in concurrent.futures.as_completed(future_to_entry):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                break
        for future, entry in future_to_entry.items():
            if future.cancelled():
                stats['num_interrupted'] += 1
            else:
                if future.result():
                    stats['num_reencoded'] += 1
                else:
                    stats['num_failed'] += 1
                    stats['failed_reencodes'].append(entry.library_path)

    PrintReencodeSummary(stats, flag.Exit())

    TimeCommand(start_time, "Reencoding library", LogLevel.INFO)

def PrintRepadSummary(stats, early_exit) -> None:
    repad_result = "Library repad interrupted:" if early_exit else "Library repad complete:"

    if stats['num_not_checked'] > 0:
        not_checked_summary = f"{stats['num_not_checked']} not checked for padding\n"
    else:
        not_checked_summary = ""

    if stats['num_padding_ok'] > 0:
        checked_summary = f"{stats['num_padding_ok']} checked and had acceptable padding\n"
    else:
        checked_summary = ""

    if stats['num_failed'] > 0:
        summary_log_level = LogLevel.WARN
        repad_fail = f"\n{fmt.WARNING}{stats['num_failed']} not repadded due to errors{fmt.ENDC}"
        flag.SetExitCode(ExitCode.WARN)
    else:
        summary_log_level = LogLevel.INFO
        repad_fail = ""

    if early_exit:
        interrupted_summary = f"\n{stats['num_interrupted']} interrupted"
    else:
        interrupted_summary = ""

    Log(summary_log_level, f"{repad_result}\n" \
                           f"{stats['num_total']} total FLACs\n" + \
                           not_checked_summary + \
                           checked_summary + \
                           f"{stats['num_repadded']} repadded" + \
                           repad_fail + \
                           interrupted_summary)

    if stats['failed_repads']:
        PrintFailureList('Failed repads:', stats['failed_repads'])

    if early_exit:
        flag.SaveAndQuit()

def RepadLibrary() -> None:
    start_time = time()

    Log(LogLevel.INFO, f"Re-padding FLACs in {cfg['formatted_library_path']}")

    stats = {
        "num_not_checked": 0,
        "num_repadded": 0,
        "num_padding_ok": 0,
        "num_failed": 0,
        "num_interrupted": 0,
        "num_total": 0,
        "failed_repads": []
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        future_to_entry = {}
        for entry in cache.flacs:
            if entry.present_in_last_scan:
                stats['num_total'] += 1
                if args.force or \
                   args.force_repad or \
                   entry.fingerprint_on_last_scan != entry.fingerprint_on_last_repad:
                    future_to_entry[executor.submit(RepadFlac, entry)] = entry
        for future in concurrent.futures.as_completed(future_to_entry):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                break
        stats['num_not_checked'] = stats['num_total'] - len(future_to_entry)
        for future, entry in future_to_entry.items():
            if future.cancelled():
                stats['num_interrupted'] += 1
            else:
                repad_attempted, command_successful = future.result()
                if repad_attempted and command_successful:
                    stats['num_repadded'] += 1
                elif not repad_attempted and command_successful:
                    stats['num_padding_ok'] += 1
                else:
                    stats['num_failed'] += 1
                    stats['failed_repads'].append(entry.library_path)

    PrintRepadSummary(stats, flag.Exit())

    TimeCommand(start_time, "Repadding library", LogLevel.INFO)

def RemoveElementsFromSequence(sequence, element_indexes) -> None:
    # Remove in reverse order to avoid decrementing element indices
    element_indexes.sort(reverse=True)
    for index in element_indexes:
        sequence.pop(index)

def RemoveOrphanedFilesFromPortable() -> None:
    start_time = time()

    Log(LogLevel.INFO, f"Removing orphaned files from {cfg['formatted_output_library_path']}")

    stats = {
        "num_dirs_removed": 0,
        "num_files_removed": 0,
        "num_transcodes_removed": 0
    }

    removal_list = []
    for index, entry in enumerate(cache.dirs):
        if not entry.present_in_last_scan:
            deletion_str = f"Delete {entry.formatted_portable_path}"
            if args.dry_run:
                Log(LogLevel.TRACE, f"Dry run: {deletion_str}")
            else:
                if os.path.isdir(entry.portable_path):
                    shutil.rmtree(entry.portable_path)
                    Log(LogLevel.TRACE, deletion_str)
                else:
                    Log(LogLevel.DEBUG, f"Directory was removed outside of script: {entry.formatted_portable_path}")
                removal_list.append(index)
            stats['num_dirs_removed'] += 1
        if flag.Exit():
            RemoveElementsFromSequence(cache.dirs, removal_list)
            flag.SaveAndQuit()
    RemoveElementsFromSequence(cache.dirs, removal_list)

    # Some orphaned files have already been deleted at this point due to the above shutil.rmtree() call
    # It's simpler and faster to not bookkeep the changes in the cache as they occur
    # Instead naively continue and use missing_ok=True when deleting files

    removal_list = []
    for index, entry in enumerate(cache.files):
        if not entry.present_in_last_scan:
            deletion_str = f"Delete {entry.formatted_portable_path}"
            if args.dry_run:
                Log(LogLevel.TRACE, f"Dry run: {deletion_str}")
            else:
                Log(LogLevel.TRACE, deletion_str)
                Path.unlink(Path(entry.portable_path), missing_ok=True)
                removal_list.append(index)
            stats['num_files_removed'] += 1
        if flag.Exit():
            RemoveElementsFromSequence(cache.dirs, removal_list)
            flag.SaveAndQuit()
    RemoveElementsFromSequence(cache.files, removal_list)

    removal_list = []
    for index, entry in enumerate(cache.flacs):
        if not entry.present_in_last_scan:
            deletion_str = f"Delete {entry.formatted_portable_path}"
            if args.dry_run:
                Log(LogLevel.TRACE, f"Dry run: {deletion_str}")
            else:
                Log(LogLevel.TRACE, deletion_str)
                Path.unlink(Path(entry.portable_path), missing_ok=True)
                removal_list.append(index)
            stats['num_transcodes_removed'] += 1
        if flag.Exit():
            RemoveElementsFromSequence(cache.dirs, removal_list)
            flag.SaveAndQuit()
    RemoveElementsFromSequence(cache.flacs, removal_list)

    Log(LogLevel.INFO, f"Orphaned file deletion complete:\n" \
                       f"{stats['num_dirs_removed']} dirs deleted\n" \
                       f"{stats['num_files_removed']} files deleted\n" \
                       f"{stats['num_transcodes_removed']} transcodes deleted")

    TimeCommand(start_time, "Removing orphaned files", LogLevel.INFO)

def PrintMirrorAndTranscodeSummary(stats, early_exit=False) -> None:
    mirror_and_transcode_result = "File mirroring/transcoding interrupted:" if early_exit else "File mirroring/transcoding complete:"

    if stats["num_file_mirrors_failed"] > 0:
        summary_log_level = LogLevel.WARN
        mirror_fail = f"\n{fmt.WARNING}Files failed to mirror:                   {stats['num_file_mirrors_failed']}{fmt.ENDC}"
        flag.SetExitCode(ExitCode.WARN)
    else:
        summary_log_level = LogLevel.INFO
        mirror_fail = ""

    if stats["num_flac_transcodes_failed"] > 0:
        summary_log_level = LogLevel.WARN
        transcode_fail = f"\n{fmt.WARNING}Flacs failed to transcode:                {stats['num_flac_transcodes_failed']}{fmt.ENDC}"
        flag.SetExitCode(ExitCode.WARN)
    else:
        summary_log_level = LogLevel.INFO
        transcode_fail = ""

    Log(summary_log_level, f"{mirror_and_transcode_result}\n" \
                           f"Directories mirrored (new/total):         {stats['num_dirs_mirrored']}/{len(cache.dirs)}\n" \
                           f"Files mirrored (new/interrupted/total):   {stats['num_file_mirrors_succeeded']}/{stats['num_file_mirrors_interrupted']}/{len(cache.files)}\n" \
                           f"Flacs transcoded (new/interrupted/total): {stats['num_flac_transcodes_succeeded']}/{stats['num_flac_transcodes_interrupted']}/{len(cache.flacs)}" + \
                           mirror_fail + \
                           transcode_fail)

    if stats["failed_mirrors"]:
        PrintFailureList('Failed mirrors:', stats["failed_mirrors"])
    if stats["failed_transcodes"]:
        PrintFailureList('Failed transcodes:', stats["failed_transcodes"])

    if early_exit:
        flag.SaveAndQuit()

def MirrorFile(entry) -> bool:
    file_mirror_str = f"{entry.formatted_path} -> {entry.formatted_portable_path}"
    if args.dry_run:
        Log(LogLevel.DEBUG, f"Dry run: {file_mirror_str}")
    else:
        Log(LogLevel.DEBUG, file_mirror_str)
        try:
            Path.unlink(Path(entry.portable_path), missing_ok=True)
            if DetectPlaylist(entry.library_path):
                if not ConvertPlaylist(entry.library_path, entry.portable_path, file_mirror_str):
                    return False
            else:
                match cfg["file_mirror_method"]:
                    case "copy":
                        shutil.copy2(entry.library_path, entry.portable_path)
                    case "soft_link":
                        os.symlink(entry.library_path, entry.portable_path)
                    case "hard_link":
                        os.link(entry.library_path, entry.portable_path)
                    case _:
                        Log(LogLevel.ERROR, f"SHOULD NOT HAPPEN: Invalid file mirror method: {cfg['file_mirror_method']}")

            entry.fingerprint_on_last_mirror = entry.fingerprint_on_last_scan
        except OSError as exc:
            Log(LogLevel.WARN, f"Error when mirroring {file_mirror_str}: {exc}")
            return False

    return True

def MirrorLibrary() -> None:
    start_time = time()

    Log(LogLevel.INFO, f"Mirroring/transcoding files to portable library {cfg['formatted_output_library_path']}")

    stats = {
        "num_dirs_mirrored": 0,
        "num_file_mirrors_succeeded": 0,
        "num_file_mirrors_interrupted": 0,
        "num_file_mirrors_failed": 0,
        "num_flac_transcodes_succeeded": 0,
        "num_flac_transcodes_interrupted": 0,
        "num_flac_transcodes_failed": 0,
        "failed_mirrors": [],
        "failed_transcodes": []
    }

    # Mirror directories
    for entry in cache.dirs:
        if not entry.mirrored or args.force:
            dir_mirror_str = f"{entry.formatted_path} -> {entry.formatted_portable_path}"
            if args.dry_run:
                Log(LogLevel.DEBUG, f"Dry run: {dir_mirror_str}")
            else:
                Log(LogLevel.DEBUG, dir_mirror_str)
                os.makedirs(entry.portable_path, exist_ok=True)
                entry.mirrored = True
            stats["num_dirs_mirrored"] += 1
        # Do not exit on signals in the loop because loop should always be fast; no huge problem to finish

    if flag.Exit():
        PrintMirrorAndTranscodeSummary(stats, early_exit=True)

    # Mirror non-flac files
    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        future_to_entry = {}
        for entry in cache.files:
            if entry.fingerprint_on_last_mirror != entry.fingerprint_on_last_scan or args.force:
                future_to_entry[executor.submit(MirrorFile, entry)] = entry
        for future in concurrent.futures.as_completed(future_to_entry):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                break
        for future, entry in future_to_entry.items():
            if future.cancelled():
                stats["num_file_mirrors_interrupted"] += 1
            elif future.result():
                stats["num_file_mirrors_succeeded"] += 1
            else:
                stats["num_file_mirrors_failed"] += 1
                stats["failed_mirrors"].append(entry.library_path)

    if flag.Exit():
        PrintMirrorAndTranscodeSummary(stats, early_exit=True)

    # Mirror flacs
    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        future_to_entry = {}
        for entry in cache.flacs:
            if ((entry.fingerprint_on_last_transcode != entry.fingerprint_on_last_scan) and \
                not entry.transcode_ignore_last_fingerprint_change) or \
               args.force or \
               (args.transcode_on_update and entry.opus_codec_on_last_transcode != opus_version):
                future_to_entry[executor.submit(TranscodeFlac, entry)] = entry
        for future in concurrent.futures.as_completed(future_to_entry):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                break
        for future, entry in future_to_entry.items():
            if future.cancelled():
                stats["num_flac_transcodes_interrupted"] += 1
            else:
                if future.result():
                    stats["num_flac_transcodes_succeeded"] += 1
                else:
                    stats["num_flac_transcodes_failed"] += 1
                    stats["failed_transcodes"].append(entry.library_path)

    PrintMirrorAndTranscodeSummary(stats, flag.Exit())

    TimeCommand(start_time, "Mirroring/transcoding files", LogLevel.INFO)

def ListOrphanedEntries() -> None:
    start_time = time()

    Log(LogLevel.INFO, f"Listing orphaned files to be removed from {cfg['formatted_output_library_path']}")

    num_orphaned_dirs = 0
    num_orphaned_files = 0
    num_orphaned_flacs = 0

    first_file_index = len(cache.dirs)
    first_flac_index = len(cache.dirs) + len(cache.files)

    for index, entry in enumerate(chain(cache.dirs, cache.files, cache.flacs)):
        if not entry.present_in_last_scan:
            Log(LogLevel.INFO, entry.formatted_path)
            entry.present_in_last_scan = False
            if index < first_file_index:
                num_orphaned_dirs += 1
            elif index < first_flac_index:
                num_orphaned_files += 1
            else:
                num_orphaned_flacs += 1

    if num_orphaned_dirs or num_orphaned_files or num_orphaned_flacs:
        Log(LogLevel.INFO, f"Found orphaned status entries:\n" \
                           f"{num_orphaned_dirs} dirs\n" \
                           f"{num_orphaned_files} files\n" \
                           f"{num_orphaned_flacs} flacs")
        Log(LogLevel.INFO, "Orphaned entries and the mirrored files they point to will be removed in the next mirror command")
    else:
        Log(LogLevel.INFO, "No orphaned entries found")

    TimeCommand(start_time, "Listing orphaned status entries", LogLevel.INFO)

def ListEntries() -> None:
    start_time = time()

    Log(LogLevel.INFO, f"Listing all scanned files in {cfg['formatted_library_path']}")

    num_dirs = 0
    num_files = 0
    num_flacs = 0

    first_file_index = len(cache.dirs)
    first_flac_index = len(cache.dirs) + len(cache.files)

    for index, entry in enumerate(chain(cache.dirs, cache.files, cache.flacs)):
        Log(LogLevel.INFO, entry.formatted_path)
        entry.present_in_last_scan = False
        if index < first_file_index:
            num_dirs += 1
        elif index < first_flac_index:
            num_files += 1
        else:
            num_flacs += 1

    if num_dirs or num_files or num_flacs:
        Log(LogLevel.INFO, f"Status entries:\n" \
                           f"{num_dirs} dirs\n" \
                           f"{num_files} files\n" \
                           f"{num_flacs} flacs")
    else:
        Log(LogLevel.INFO, "No status entries found")

    TimeCommand(start_time, "Listing status entries", LogLevel.INFO)

def SaveCacheBackup() -> None:
    library_status_backup_path = cfg["library_status_path"] + ".bak"
    shutil.copy2(cfg["library_status_path"], library_status_backup_path)
    Log(LogLevel.INFO, f"Saved backup of current library status file at {library_status_backup_path}")

def ParseArgs() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Maintain mirror image music library, transcoded to Opus")
    parser.add_argument("-v", "--version", action='version', version='%(prog)s 1.0.0')
    subparsers = parser.add_subparsers(required=True)

    parser_reencode = subparsers.add_parser('reencode', help='reencode flacs in library (default behavior is to only reencode new flacs, options define additional cases to reencode)')
    parser_reencode.add_argument("-u", "--reencode-on-update", action="store_true", help="reencode flacs that have already been reencoded, if flac codec has been updated")
    parser_reencode.add_argument("-c", "--reencode-on-change", action="store_true", help="reencode flacs that have already been reencoded, if file fingerprint has changed")
    parser_reencode.add_argument("-f", "--force", action="store_true", help="reencode every flac, mirror every file, even if fingerprint is unchanged")
    parser_reencode.add_argument("-p", "--force-repad", action="store_true", help="Check every flac for a repad oppurtunity, even if fingerprint is unchanged")
    parser_reencode.add_argument("-d", "--dry-run", action="store_true", help="show what actions would be taken")
    parser_reencode.add_argument("-k", "--skip-scan", action="store_true", help="skip automatic scan before reencode (warning: ensure that library is unmodified since last scan)")
    parser_reencode.set_defaults(func=reencode_library)

    parser_transcode = subparsers.add_parser('mirror', help='mirror library')
    parser_transcode.add_argument("-u", "--transcode-on-update", action="store_true", help="retranscode flacs that have already been transcoded, if opus codec has been updated")
    parser_transcode.add_argument("-f", "--force", action="store_true", help="mirror every file, even if fingerprint is unchanged")
    parser_transcode.add_argument("-d", "--dry-run", action="store_true", help="show what actions would be taken")
    parser_transcode.add_argument("-k", "--skip-scan", action="store_true", help="skip automatic scan before mirror (warning: ensure that library is unmodified since last scan)")
    parser_transcode.set_defaults(func=mirror_library)

    parser_playlists = subparsers.add_parser('convert_playlists', help='convert playlists to reference mirrored names (.flac -> .opus)')
    parser_playlists.add_argument("-d", "--dry-run", action="store_true", help="show what actions would be taken")
    parser_playlists.set_defaults(func=convert_playlists)

    parser_scan = subparsers.add_parser('scan', help='scan library for changes')
    parser_scan.add_argument("-t", "--test", action="store_true", help="test flacs")
    parser_scan.add_argument("-T", "--test-force", action="store_true", help="test all flacs")
    parser_scan.add_argument("-u", "--retest-on-update", action="store_true", help="test flacs, and re-test on flac codec update")
    parser_scan.set_defaults(func=scan_library)

    parser_list = subparsers.add_parser('list', help='list cache entries')
    parser_list.add_argument("-o", "--orphan-only", action="store_true", help="only list orphaned entries")
    parser_list.set_defaults(func=list_cache)

    return parser.parse_args()

def reencode_library() -> None:
    SaveCacheBackup()
    ReadCache()
    if not args.skip_scan:
        ScanLibrary()
        CheckForOrphanedCache()
    ReencodeLibrary()
    if cfg["check_padding"]:
        RepadLibrary()
    WriteCache()

def mirror_library() -> None:
    SaveCacheBackup()
    ReadCache()
    if not args.skip_scan:
        ScanLibrary()
        CheckForOrphanedCache()
    RemoveOrphanedFilesFromPortable()
    MirrorLibrary()
    WriteCache()

def convert_playlists() -> None:
    Log(LogLevel.INFO, f"Converting playlists in {cfg['formatted_library_playlist_path']}")

    # Simpler to recreate all playlists each run; surely no one has enough playlists that this takes time
    if os.path.isdir(cfg["portable_playlist_path"]):
        for root, _dirs, files in os.walk(cfg["portable_playlist_path"]):
            for file in files:
                file_path = os.path.join(root, file)
                if DetectPlaylist(file_path):
                    Path.unlink(Path(file_path), missing_ok=True)
    else:
        os.makedirs(cfg["portable_playlist_path"])

    for root, _dirs, files in os.walk(cfg["library_playlist_path"]):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = file_path[len(cfg["library_playlist_path"]):]
            formatted_path = FormatPath(relative_path, fmt.OKGREEN)
            if DetectPlaylist(file_path):
                output_path = os.path.join(cfg["portable_playlist_path"], relative_path)
                playlist_convert_str = f"{formatted_path} -> {FormatPath(relative_path, fmt.OKBLUE)}"
                if args.dry_run:
                    Log(LogLevel.DEBUG, f"Dry run: {playlist_convert_str}")
                else:
                    Log(LogLevel.DEBUG, playlist_convert_str)
                    ConvertPlaylist(file_path, output_path, playlist_convert_str)
            else:
                Log(LogLevel.DEBUG, f"Skipping non-playlist file {formatted_path}")

            flag.SaveAndQuitIfSignalled()

def scan_library() -> None:
    SaveCacheBackup()
    ReadCache()
    ScanLibrary()
    CheckForOrphanedCache()
    WriteCache()

def list_cache() -> None:
    SaveCacheBackup()
    ReadCache()
    if args.orphan_only:
        ListOrphanedEntries()
    else:
        ListEntries()

if __name__ == '__main__':
    assert sys.version_info >= (3, 10)
    script_start = time()
    is_windows = system() == "Windows"
    print_lock = Lock()
    cpu_count = os.process_cpu_count() if sys.version_info >= (3, 13) else os.cpu_count()
    flag = GracefulExiter()
    fmt = NoFormat
    cache = []

    # ffmpeg changes stdin attributes when it is terminated
    # ffmpeg support was dropped, but no harm leaving this here in case it's still needed somehow
    if not is_windows:
        import termios
        original_stdin_attr = termios.tcgetattr(sys.stdin.fileno())

    args = ParseArgs()

    # Convenience shortcuts for test arguments
    test = hasattr(args, 'test') and args.test
    test_force = hasattr(args, 'test_force') and args.test_force
    retest_on_update = hasattr(args, 'retest_on_update') and args.retest_on_update
    test_specified = test or retest_on_update or test_force

    cfg = {}
    tmp_config = ReadConfig(CONFIG_FILE)
    if ValidateConfig(tmp_config):
        cfg = tmp_config
    else:
        Log(LogLevel.ERROR, f"Error(s) found in {CONFIG_FILE}", always_log=True)

    if cfg["num_threads"] == 0:
        cfg["num_threads"] = cpu_count
    Log(LogLevel.INFO, f"Using {cfg['num_threads']} worker threads")

    os.makedirs(cfg["output_library_path"], exist_ok=True)
    if args.func is convert_playlists:
        os.makedirs(cfg["portable_playlist_path"], exist_ok=True)

    flac_version, opus_version = CheckDependencies()
    ValidateDependencyConfigArgumentCombinations()

    flag.QuitWithoutSavingIfSignalled()
    args.func()

    TimeCommand(script_start, "MusicMirror", LogLevel.INFO)

    flag.QuitWithoutSaving()
