#!/usr/bin/env python3

import argparse
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import total_ordering
from hashlib import file_digest
from itertools import chain
import os
from pathlib import Path
from platform import system
from re import sub, MULTILINE
from shlex import quote
import shutil
import signal
import subprocess
import sys
import termios
from threading import Lock, current_thread, local
from time import time
from typing import Tuple, Dict, Union
import yaml

thread_info = local()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

@total_ordering
class LogLevel(Enum):
    ERROR    = 0
    WARN     = 1
    INFO     = 2
    DEBUG    = 3
    TRACE    = 4
    def __lt__(self, other) -> Union[bool, NotImplemented]:
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

def SetThreadName() -> None:
    global thread_info

    thread_name = current_thread().name
    if thread_name == "MainThread":
        thread_info.name = "Main"
    else:
        thread_num = thread_name.split('_')[-1].zfill(2)
        thread_info.name = f"WT{thread_num}"

def Log(level, log) -> None:
    global cfg
    global print_lock
    global thread_info

    exit_early = False
    if level <= cfg["log_level"]:
        timestamp = str(datetime.now()).split(" ")[1]

        if not hasattr(thread_info, 'name'):
            SetThreadName()

        match level:
            case LogLevel.ERROR:
                full_log = f"[{timestamp}][{thread_info.name}][{bcolors.FAIL}{bcolors.BOLD}ERROR{bcolors.ENDC}] {log}"
                exit_early = True
            case LogLevel.WARN:
                full_log = f"[{timestamp}][{thread_info.name}][{bcolors.WARNING}{bcolors.BOLD}WARN{bcolors.ENDC} ] {log}"
            case LogLevel.INFO:
                full_log = f"[{timestamp}][{thread_info.name}][{bcolors.OKGREEN}INFO{bcolors.ENDC} ] {log}"
            case LogLevel.DEBUG:
                full_log = f"[{timestamp}][{thread_info.name}][{bcolors.OKBLUE}DEBUG{bcolors.ENDC}] {log}"
            case LogLevel.TRACE:
                full_log = f"[{timestamp}][{thread_info.name}][{bcolors.OKCYAN}TRACE{bcolors.ENDC}] {log}"
            case _:
                QuitWithoutSaving(f"Invalid log level '{level}' for log '{log}'")

        print_lock.acquire()
        print(full_log)
        print_lock.release()

        if exit_early:
            QuitWithoutSaving(1)

def ReadConfig(config_path) -> dict:
    global cfg

    cfg["log_level"] = LogLevel.INFO

    Log(LogLevel.INFO, f"Reading configuration settings from {FormatPath(config_path)}")

    with open(config_path) as stream:
        try:
            cfg_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            Log(LogLevel.ERROR, str(exc))

    Log(LogLevel.INFO, f"Configuration settings read from {config_path}")

    return cfg_dict

def ValidateConfigDictKey(cfg, key_name, expected_type) -> bool:
    if key_name in cfg:
        if isinstance(cfg[key_name], expected_type):
            return True
        else:
            Log(LogLevel.WARN, f"Config option {key_name} has unexpected type {type(cfg[key_name])}")
            return False
    else:
        Log(LogLevel.WARN, f"Config option {key_name} not found")
        return False

def ValidateConfig(cfg) -> bool:

    ok = True

    ok = ok and ValidateConfigDictKey(cfg, "log_level", str)
    ok = ok and ValidateConfigDictKey(cfg, "library_status_path", str)
    ok = ok and ValidateConfigDictKey(cfg, "library_path", str)
    ok = ok and ValidateConfigDictKey(cfg, "output_library_path", str)
    ok = ok and ValidateConfigDictKey(cfg, "library_playlist_path", str)
    ok = ok and ValidateConfigDictKey(cfg, "portable_playlist_path", str)
    ok = ok and ValidateConfigDictKey(cfg, "opus_bitrate", int)
    ok = ok and ValidateConfigDictKey(cfg, "allow_library_modification", bool)
    ok = ok and ValidateConfigDictKey(cfg, "use_hash_as_fingerprint", bool)
    ok = ok and ValidateConfigDictKey(cfg, "num_threads", int)
    ok = ok and ValidateConfigDictKey(cfg, "file_mirror_method", str)
    ok = ok and ValidateConfigDictKey(cfg, "log_full_paths", bool)
    ok = ok and ValidateConfigDictKey(cfg, "ignore_hidden", bool)
    ok = ok and ValidateConfigDictKey(cfg, "check_padding", bool)
    ok = ok and ValidateConfigDictKey(cfg, "min_padding_size", int)
    ok = ok and ValidateConfigDictKey(cfg, "max_padding_size", int)
    ok = ok and ValidateConfigDictKey(cfg, "target_padding_size", int)

    if not ok:
        return False

    ok = ValidateConfigPaths(cfg)

    if cfg["log_level"] == "error":
        cfg["log_level"] = LogLevel.ERROR
    elif cfg["log_level"] == "warn":
        cfg["log_level"] = LogLevel.WARN
    elif cfg["log_level"] == "info":
        cfg["log_level"] = LogLevel.INFO
    elif cfg["log_level"] == "debug":
        cfg["log_level"] = LogLevel.DEBUG
    elif cfg["log_level"] == "trace":
        cfg["log_level"] = LogLevel.TRACE
    else:
        Log(LogLevel.WARN, f"Invalid log level {cfg["log_level"]}")
        ok = False
    if (ok):
        Log(LogLevel.INFO, f"Log level set to {cfg["log_level"]}")

    # Do not validate opus max bitrate here because valid range depends on the number of audio channels. Leave it up to the user to get it right
    if cfg["opus_bitrate"] <= 0:
        Log(LogLevel.WARN, f"Opus bitrate must be a positive integer")
        ok = False

    if cfg["min_padding_size"] < 0:
        Log(LogLevel.WARN, f"Min flac padding size cannot be negative")
        ok = False

    if cfg["max_padding_size"] < 0:
        Log(LogLevel.WARN, f"Max flac padding size cannot be negative")
        ok = False

    if cfg["target_padding_size"] < 0:
        Log(LogLevel.WARN, f"Target flac padding size cannot be negative")
        ok = False

    if cfg["min_padding_size"] > cfg["target_padding_size"]:
        Log(LogLevel.WARN, f"min_padding_size cannot be greater than target_padding_size")
        ok = False

    if cfg["min_padding_size"] > cfg["max_padding_size"]:
        Log(LogLevel.WARN, f"min_padding_size cannot be greater than max_padding_size")
        ok = False

    if cfg["target_padding_size"] > cfg["max_padding_size"]:
        Log(LogLevel.WARN, f"target_padding_size cannot be greater than max_padding_size")
        ok = False

    if cfg["num_threads"] > os.process_cpu_count():
        Log(LogLevel.WARN, f"Number of worker threads ({cfg["num_threads"]}) cannot exceed number of cores available to process ({os.process_cpu_count()})")
        ok = False

    if cfg["file_mirror_method"] != "copy" and cfg["file_mirror_method"] != "soft_link" and cfg["file_mirror_method"] != "hard_link":
        Log(LogLevel.WARN, f"Invalid file mirror method {cfg["file_mirror_method"]}. Supported options are copy, soft_link, hard_link")
        ok = False

    return ok

def RestoreStdinAttr() -> None:
    global original_stdin_attr

    termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, original_stdin_attr)

def SaveAndQuit(exit_arg=None) -> None:
    WriteCache()
    RestoreStdinAttr()
    sys.exit(exit_arg)

def QuitWithoutSaving(exit_arg=None) -> None:
    RestoreStdinAttr()
    sys.exit(exit_arg)

class GracefulExiter():

    def __init__(self) -> None:
        self.state = False
        signal.signal(signal.SIGINT, self.ChangeState)
        signal.signal(signal.SIGHUP, self.ChangeState)
        signal.signal(signal.SIGTERM, self.ChangeState)

    def ChangeState(self, signum, frame) -> None:
        global print_lock

        signal_name = signal.Signals(signum).name
        signal_log = f"\nReceived signal {signal_name}; finishing processing"
        if signal.Signals(signum) == signal.SIGINT:
            signal_log += " (repeat to exit now)"
        print_lock.acquire()
        print(signal_log)
        print_lock.release()
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        self.state = True

    def Exit(self) -> bool:
        return self.state

    def SaveAndQuitIfSignalled(self, exit_arg=None) -> None:
        if self.Exit():
            SaveAndQuit(exit_arg)

    def QuitWithoutSavingIfSignalled(self, exit_arg=None) -> None:
        if self.Exit():
            QuitWithoutSaving(exit_arg)

def SetUpChildSignals() -> None:
    # Ignore these signals in child processes to avoid leaving temp files around if in the middle of a transcode/reencode
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

# Leave the date off the end of the vendor string; cannot get the date without manually making a big version->date map
def ConvertFlacVersionToVendorString(version) -> str:
    version_number = version.split(' ')[1]
    return f"reference libFLAC {version_number}"

def AppendPathSeparator(path) -> str:
    if not path.endswith(os.sep):
        path += os.sep
    return path

def CheckDependencies() -> None:
    global cfg
    global flac_version
    global opus_version

    try:
        flac_output = subprocess.run(['flac', '--version'], capture_output=True)
        if flac_output.returncode < 0:
            QuitWithoutSaving()
        flac_version = flac_output.stdout.decode('utf-8')[:-1]
        flac_version = ConvertFlacVersionToVendorString(flac_version)
    except subprocess.CalledProcessError as exc:
        flac_error = str(exc)

    try:
        metaflac_output = subprocess.run(['metaflac', '--version'], capture_output=True)
        if metaflac_output.returncode < 0:
            QuitWithoutSaving()
        metaflac_version = metaflac_output.stdout.decode('utf-8')[:-1]
    except subprocess.CalledProcessError as exc:
        metaflac_error = str(exc)

    try:
        opus_output = subprocess.run(['opusenc', '--version'], capture_output=True)
        if opus_output.returncode < 0:
            QuitWithoutSaving()
        opus_version = opus_output.stdout.decode('utf-8').split("\n")[0]
    except subprocess.CalledProcessError as exc:
        opus_error = str(exc)

    if not flac_version:
        Log(LogLevel.WARN, "flac codec unavailable - cannot encode, decode, or test FLACs: " + flac_error)
    if not metaflac_version:
        Log(LogLevel.WARN, "metaflac unavailable - cannot adjust padding in FLACs: " + metaflac_error)
    if not opus_version:
        Log(LogLevel.WARN, "opus codec unavailable - cannot encode Opus files: " + opus_error)

    Log(LogLevel.INFO, "Python version:   " + str(sys.version))
    if flac_version:
        Log(LogLevel.INFO, "flac version:     " + flac_version)
    if metaflac_version:
        Log(LogLevel.INFO, "metaflac version: " + metaflac_version)
    if opus_version:
        Log(LogLevel.INFO, "Opus version:     " + opus_version)

def ValidateDependencyConfigArgumentCombinations() -> None:
    global args
    global cfg
    global flac_version
    global opus_version
    global test_specified

    if test_specified and not flac_version:
        Log(LogLevel.ERROR, "flac codec unavailable to test FLACs with")

    if not cfg["allow_library_modification"] and args.func == reencode_library:
        Log(LogLevel.ERROR, "Config setting 'allow_library_modification' is disabled. Enable to allow reencoding of library")

    if not flac_version and args.func == reencode_library:
        Log(LogLevel.ERROR, "Cannot reencode library without a FLAC codec available")

    # TODO is this true?
    if not flac_version and args.func == mirror_library:
        Log(LogLevel.ERROR, "Cannot transcode portable library without a FLAC decoder available")

    if not opus_version and args.func == mirror_library:
        Log(LogLevel.ERROR, "Cannot transcode portable library without an Opus encoder available")

    # Hard links require both links to be on the same filesystem
    if hasattr(args, 'hard_link') and args.hard_link and \
       os.stat(cfg["library_path"]).st_dev != os.stat(cfg["output_library_path"]).st_dev:
        Log(LogLevel.ERROR, "To use hard links the main library and portable library must reside on the same filesystem")

def ValidateConfigPaths(cfg) -> bool:

    ok = True

    cfg["library_status_path"] = os.path.expanduser(cfg["library_status_path"])
    cfg["library_path"] = AppendPathSeparator(os.path.expanduser(cfg["library_path"]))
    cfg["output_library_path"] = AppendPathSeparator(os.path.expanduser(cfg["output_library_path"]))
    cfg["library_playlist_path"] = AppendPathSeparator(os.path.expanduser(cfg["library_playlist_path"]))
    cfg["portable_playlist_path"] = AppendPathSeparator(os.path.expanduser(cfg["portable_playlist_path"]))

    cfg["formatted_library_status_path"] = FormatPath(cfg["library_status_path"])
    cfg["formatted_library_path"] = FormatPath(cfg["library_path"], bcolors.OKGREEN)
    cfg["formatted_output_library_path"] = FormatPath(cfg["output_library_path"], bcolors.OKBLUE)
    cfg["formatted_library_playlist_path"] = FormatPath(cfg["library_playlist_path"], bcolors.OKGREEN)
    cfg["formatted_portable_playlist_path"] = FormatPath(cfg["portable_playlist_path"], bcolors.OKBLUE)

    library_status_path_obj = Path(cfg["library_status_path"])
    library_path_obj = Path(cfg["library_path"])
    output_library_path_obj = Path(cfg["output_library_path"])
    library_playlist_path_obj = Path(cfg["library_playlist_path"])
    portable_playlist_path_obj = Path(cfg["portable_playlist_path"])

    if cfg["library_path"] == "" or cfg["output_library_path"] == "":
        Log(LogLevel.WARN, f"Library path and output library path must be configured in config.yaml")
        ok = False

    if not library_status_path_obj.is_file():
        Log(LogLevel.WARN, f"Library status path {cfg["formatted_library_status_path"]} does not exist or is not a file")
        ok = False
    if not library_path_obj.is_dir():
        Log(LogLevel.WARN, f"Library path {cfg["formatted_library_path"]} does not exist or is not a directory")
        ok = False

    if cfg["output_library_path"] == cfg["library_path"]:
        Log(LogLevel.WARN, f"Output library path {cfg["formatted_output_library_path"]} matches library path {cfg["formatted_library_path"]}")
        ok = False
    if library_path_obj in output_library_path_obj.parents:
        Log(LogLevel.WARN, f"Output library path {cfg["formatted_output_library_path"]} is inside library path {cfg["formatted_library_path"]}")
        ok = False
    if output_library_path_obj in library_path_obj.parents:
        Log(LogLevel.WARN, f"Library path {cfg["formatted_library_path"]} is inside output library path {cfg["formatted_output_library_path"]}")
        ok = False

    if args.func == convert_playlists:
        if not library_playlist_path_obj.is_dir():
            Log(LogLevel.WARN, f"Library playlist path {cfg["formatted_library_playlist_path"]} does not exist or is not a directory")
            ok = False
        if not portable_playlist_path_obj.is_dir():
            Log(LogLevel.WARN, f"Portable playlist path {cfg["formatted_portable_playlist_path"]} does not exist or is not a directory")
            ok = False

        if cfg["portable_playlist_path"] == cfg["library_playlist_path"]:
            Log(LogLevel.WARN, f"Portable playlists path {cfg["formatted_portable_playlist_path"]} matches library playlist path {cfg["formatted_library_playlist_path"]}")
            ok = False
        if library_playlist_path_obj in portable_playlist_path_obj.parents:
            Log(LogLevel.WARN, f"Portable playlists path {cfg["formatted_portable_playlist_path"]} is inside library playlist path {cfg["formatted_library_playlist_path"]}")
            ok = False
        if portable_playlist_path_obj in library_playlist_path_obj.parents:
            Log(LogLevel.WARN, f"Library playlists path {cfg["formatted_library_playlist_path"]} is inside portable playlist path {cfg["formatted_portable_playlist_path"]}")
            ok = False

        if cfg["portable_playlist_path"] == cfg["output_library_path"]:
            Log(LogLevel.WARN, f"Portable playlists path {cfg["formatted_portable_playlist_path"]} matches output library playlist path {cfg["formatted_output_library_path"]}")
            ok = False
        if output_library_path_obj in portable_playlist_path_obj.parents:
            Log(LogLevel.WARN, f"Portable playlists path {cfg["formatted_portable_playlist_path"]} is inside output library path {cfg["formatted_output_library_path"]}")
            ok = False

    return ok

@dataclass
class DirEntry():
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
        global args
        global cfg

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
            Log(LogLevel.ERROR, f"SHOULD NOT HAPPEN: bad dir init arguments")

        if cfg["log_full_paths"]:
            self.formatted_path = FormatPath(self.library_path, bcolors.OKGREEN)
        else:
            self.formatted_path = FormatPath(self.path, bcolors.OKGREEN)

        if args.func == mirror_library:
            self.portable_path = os.path.join(cfg["output_library_path"], self.path)
            if cfg["log_full_paths"]:
                self.formatted_portable_path = FormatPath(self.portable_path, bcolors.OKBLUE)
            else:
                self.formatted_portable_path = FormatPath(self.path, bcolors.OKBLUE)

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
class FileEntry():
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
        global args
        global cfg

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
            Log(LogLevel.ERROR, f"SHOULD NOT HAPPEN: bad file init arguments")

        if cfg["log_full_paths"]:
            self.formatted_path = FormatPath(self.library_path, bcolors.OKGREEN)
        else:
            self.formatted_path = FormatPath(self.path, bcolors.OKGREEN)

        if args.func == mirror_library:
            self.portable_path = os.path.join(cfg["output_library_path"], self.path)
            if cfg["log_full_paths"]:
                self.formatted_portable_path = FormatPath(self.portable_path, bcolors.OKBLUE)
            else:
                self.formatted_portable_path = FormatPath(self.path, bcolors.OKBLUE)

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
class FlacEntry():
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
        global args
        global cfg

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
            Log(LogLevel.ERROR, f"SHOULD NOT HAPPEN: bad flac init arguments")

        self.quoted_path = quote(self.library_path)

        if cfg["log_full_paths"]:
            self.formatted_path = AddColor(self.quoted_path, bcolors.OKGREEN)
        else:
            self.formatted_path = FormatPath(self.path, bcolors.OKGREEN)

        if args.func == mirror_library:
            relative_portable_path = self.path[:-5] + ".opus"
            self.portable_path = os.path.join(cfg["output_library_path"], relative_portable_path)
            if cfg["log_full_paths"]:
                self.formatted_portable_path = FormatPath(self.portable_path, bcolors.OKBLUE)
            else:
                self.formatted_portable_path = FormatPath(relative_portable_path, bcolors.OKBLUE)

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
    Log(LogLevel.WARN, f"{bcolors.WARNING}{description}{bcolors.ENDC}")

def ReadCache() -> None:
    global cache
    global cfg

    start_time = time()
    cache = Cache()

    Log(LogLevel.INFO, f"Reading library status from {cfg["formatted_library_status_path"]}")

    with open(cfg["library_status_path"]) as stream:
        try:
            cache_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            Log(LogLevel.ERROR, str(exc))

    if cache_dict:
        if (cache_dict["dirs"]):
            for entry in cache_dict["dirs"].items():
                cache.dirs.append(DirEntry(saved_entry=entry))
        if (cache_dict["files"]):
            for entry in cache_dict["files"].items():
                cache.files.append(FileEntry(saved_entry=entry))
        if (cache_dict["flacs"]):
            for entry in cache_dict["flacs"].items():
                cache.flacs.append(FlacEntry(saved_entry=entry))

    TimeCommand(start_time, "Reading library status", LogLevel.INFO)

    flag.QuitWithoutSavingIfSignalled()

def WriteCache() -> None:
    global cache
    global cfg

    start_time = time()

    Log(LogLevel.INFO, f"Saving library status to {cfg["formatted_library_status_path"]}")
    with open(cfg["library_status_path"], "w") as stream:
        try:
            stream.write(yaml.dump(cache.asdict()))
        except yaml.YAMLError as exc:
            Log(LogLevel.ERROR, str(exc))

    TimeCommand(start_time, "Writing library status", LogLevel.INFO)

def FormatPath(path, color='') -> str:
    color_reset = bcolors.ENDC if color else ''
    return f'{color}{quote(path)}{color_reset}'

def AddColor(text, color='') -> str:
    color_reset = bcolors.ENDC if color else ''
    return f'{color}{text}{color_reset}'

def DetectPlaylist(file_path) -> bool:
    file_extension = file_path.split(".")[-1]
    return file_extension == "m3u" or file_extension == "m3u8"

def ConvertPlaylist(file_path, output_path, playlist_convert_str) -> bool:
    try:
        possible_encodings = ["utf-8", "cp1252", "latin1"]
        for encoding in possible_encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as in_playlist:
                    data = in_playlist.read()
            except UnicodeDecodeError:
                Log(LogLevel.TRACE, f"Could not decode playlist {playlist_convert_str} with encoding {encoding}")
                continue
        if data:
            updated_string = sub(r".flac$", ".opus", data, flags=MULTILINE)
        else:
            Log(LogLevel.WARN, f"Could not decode playlist {playlist_convert_str}")
            return False

        output_file = Path(output_path)
        output_file.parent.mkdir(exist_ok=True, parents=True)
        output_file.write_text(updated_string)
        return True
    except OSError as exc:
        Log(LogLevel.WARN, f"Error when converting playlist {playlist_convert_str}: {exc}")
        return False

def TestFlac(file_path) -> Tuple[bool, str]:
    global cfg

    test_error = ''

    test_result = subprocess.run(['flac', '-t', '-w', '-s', file_path], capture_output=True)
    if test_result.returncode != 0:
        test_error = test_result.stderr.decode("utf-8").split(".flac: ")[-1][:-1]

    if test_error:
        status = f"{bcolors.WARNING}FLAC test failed:\n{test_error}{bcolors.ENDC}"
        return False, status
    status = ""
    return True, status

def ConditionallyRunFlacTest(entry, is_new, is_modified, fingerprint) -> Tuple[bool, bool, str]:
    global flac_version
    global retest_on_update
    global test
    global test_force
    global test_specified

    test_ran = False
    status = ""
    if (test_specified and is_new) or \
        ((test and (is_new or is_modified or not entry.fingerprint_on_last_test)) or \
         (retest_on_update and (fingerprint != entry.fingerprint_on_last_test or entry.flac_codec_on_last_test != flac_version)) or \
         test_force):
        entry.test_pass, status = TestFlac(entry.library_path)
        entry.fingerprint_on_last_test = fingerprint
        entry.flac_codec_on_last_test = flac_version
        test_ran = True

    return test_ran, entry.test_pass, status

def CalculateFileHash(file_path) -> str:
    with open(file_path, "rb") as f:
        return file_digest(f, 'sha224').hexdigest()

def CalculateFingerprint(file_path) -> str:
    global cfg

    if cfg["use_hash_as_fingerprint"]:
        return CalculateFileHash(file_path)
    else:
        return str(datetime.fromtimestamp(Path(file_path).stat().st_mtime, timezone.utc))

# Returns whether successful
def ReencodeFlac(entry) -> bool:
    global cfg
    global flac_version
    global log_prefix_indent

    reencode_log = f"Reencode {entry.formatted_path}"

    if args.dry_run:
        Log(LogLevel.TRACE, f"Dry run: {reencode_log}")
        return True

    # Write to a temp file first, then overwrite if encoding successful
    tmp_path = entry.library_path + ".tmp"
    flac_args = ['flac', '--silent', '--best', '--verify', f'--padding={cfg["target_padding_size"]}', '--no-preserve-modtime', entry.library_path, '-o', tmp_path]

    with subprocess.Popen(flac_args, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, preexec_fn=SetUpChildSignals) as p:
        try:
            outs, errs = p.communicate(timeout=60)
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
                    reencode_log += f"\n{log_prefix_indent}    New fingerprint: {fingerprint}"
                if errs:
                    reencode_log_level = LogLevel.WARN
                    reencode_log += f"\n{bcolors.WARNING}FLAC reencode passed with warnings:" \
                                    f"\n{errs.removesuffix('\n')}{bcolors.ENDC}"
                else:
                    reencode_log_level = LogLevel.DEBUG
                Log(reencode_log_level, reencode_log)
                return True

            else:
                Path.unlink(tmp_path, missing_ok=True)
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{reencode_log}\n" \
                                       f"{bcolors.WARNING}FLAC reencode terminated by signal {-1 * p.returncode}{bcolors.ENDC}")
                    return False
                else:
                    Log(LogLevel.WARN, f"{reencode_log}\n" \
                                       f"{bcolors.WARNING}FLAC reencode failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix('\n')}{bcolors.ENDC}")
                    return False

        except subprocess.TimeoutExpired:
            Path.unlink(tmp_path, missing_ok=True)
            Log(LogLevel.WARN, f"Reencode subprocess for {reencode_log} timed out")
            return False

class RepadAction(Enum):
    NONE           = 0
    MERGE_AND_SORT = 1
    RESIZE         = 2

# Returns whether successful and the repad action to take. If not successful, action is always NONE
def CheckIfRepadNecessary(entry) -> Tuple[bool, RepadAction]:
    global cfg
    global log_prefix_indent

    repad_check_log = f"Padding check for {entry.formatted_path}"

    metaflac_args = ['metaflac', '--list', '--block-type=PADDING', entry.library_path]

    with subprocess.Popen(metaflac_args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=SetUpChildSignals) as p:
        try:
            outs, errs = p.communicate(timeout=60)
            if p.returncode == 0:
                outlines = outs.splitlines()

                padding_block_length_lines = [line for line in outlines if line.startswith('  length:')]
                num_padding_blocks = len(padding_block_length_lines)
                total_padding_size_bytes = 0
                for line in padding_block_length_lines:
                    total_padding_size_bytes += int(line.split(': ')[-1])

                if total_padding_size_bytes > cfg["max_padding_size"] or total_padding_size_bytes < cfg["min_padding_size"]:
                    Log(LogLevel.DEBUG, f"{repad_check_log}\n" \
                                        f"{log_prefix_indent}flac has {total_padding_size_bytes} bytes of padding")
                    return True, RepadAction.RESIZE
                elif num_padding_blocks > 1:
                    Log(LogLevel.DEBUG, f"{repad_check_log}\n" \
                                        f"{log_prefix_indent}flac has {num_padding_blocks} padding blocks")
                    return True, RepadAction.MERGE_AND_SORT
                elif not '  is last: true' in outlines:
                    Log(LogLevel.DEBUG, f"{repad_check_log}\n" \
                                        f"{log_prefix_indent}flac padding is not last block")
                    return True, RepadAction.MERGE_AND_SORT
                else:
                    return True, RepadAction.NONE
            else:
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{repad_check_log}\n" \
                                       f"{bcolors.WARNING}metaflac padding check terminated by signal {-1 * p.returncode}{bcolors.ENDC}")
                    return False, RepadAction.NONE
                else:
                    Log(LogLevel.WARN, f"{repad_check_log}\n" \
                                       f"{bcolors.WARNING}metaflac padding check failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix('\n')}{bcolors.ENDC}")
                    return False, RepadAction.NONE

        except subprocess.TimeoutExpired:
            Log(LogLevel.WARN, f"metaflac padding check subprocess for {repad_check_log} timed out")
            return False, RepadAction.NONE

# Returns whether repad attempted and whether successful (either repad check or repad itself can fail)
def RepadFlac(entry) -> Tuple[bool, bool]:
    global cfg
    global log_prefix_indent

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
                            f"metaflac --add-padding={cfg["max_padding_size"]} {entry.quoted_path}"
            repad_description = "resize padding"
            use_shell = True
        case RepadAction.NONE:
            if repad_check_ok:
                entry.fingerprint_on_last_repad = entry.fingerprint_on_last_scan
            return False, repad_check_ok

    repad_log = f"Repad {entry.formatted_path} ({repad_description})"

    if args.dry_run:
        Log(LogLevel.TRACE, f"Dry run: {repad_log}")
        return True, False

    with subprocess.Popen(metaflac_args, shell=use_shell, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, preexec_fn=SetUpChildSignals) as p:
        try:
            outs, errs = p.communicate(timeout=60)
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
                    repad_log += f"\n{log_prefix_indent}    New fingerprint: {fingerprint}"
                if errs:
                    repad_log_level = LogLevel.WARN
                    repad_log += f"\n{bcolors.WARNING}FLAC repad passed with warnings:" \
                                 f"\n{errs.removesuffix('\n')}{bcolors.ENDC}"
                else:
                    repad_log_level = LogLevel.DEBUG
                Log(repad_log_level, repad_log)
                return True, not errs

            else:
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{repad_log}\n" \
                                       f"{bcolors.WARNING}FLAC repad terminated by signal {-1 * p.returncode}{bcolors.ENDC}")
                    return True, False
                else:
                    Log(LogLevel.WARN, f"{repad_log}\n" \
                                       f"{bcolors.WARNING}FLAC repad failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix('\n')}{bcolors.ENDC}")
                    return True, False

        except subprocess.TimeoutExpired:
            Log(LogLevel.WARN, f"Repad subprocess for {repad_log} timed out")
            return True, False

# Returns whether successful
def TranscodeFlac(entry) -> bool:
    global cfg
    global opus_version

    transcode_log = f"{entry.formatted_path} -> {entry.formatted_portable_path}"

    if args.dry_run:
        Log(LogLevel.TRACE, f"Dry run: {transcode_log}")
        return True, se

    transcode_args = ['opusenc', '--quiet', '--music', '--bitrate', str(cfg["opus_bitrate"]), entry.library_path, entry.portable_path]

    with subprocess.Popen(transcode_args, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, preexec_fn=SetUpChildSignals) as p:
        try:
            outs, errs = p.communicate(timeout=60)
            if p.returncode == 0:

                entry.fingerprint_on_last_transcode = entry.fingerprint_on_last_scan
                entry.opus_codec_on_last_transcode = opus_version

                if errs:
                    transcode_log_level = LogLevel.WARN
                    transcode_log += f"\n{bcolors.WARNING}Transcode passed with warnings:" \
                                     f"\n{errs.removesuffix('\n')}{bcolors.ENDC}"
                else:
                    transcode_log_level = LogLevel.DEBUG
                Log(transcode_log_level, transcode_log)
                return True

            else:
                if p.returncode < 0:
                    Log(LogLevel.WARN, f"{transcode_log}\n" \
                                       f"{bcolors.WARNING}Transcode terminated by signal {-1 * p.returncode}{bcolors.ENDC}")
                    return False
                else:
                    Log(LogLevel.WARN, f"{transcode_log}\n" \
                                       f"{bcolors.WARNING}Transcode failed with return code {p.returncode}:\n" \
                                       f"{errs.removesuffix('\n')}{bcolors.ENDC}")
                    return False

        except subprocess.TimeoutExpired:
            Log(LogLevel.WARN, f"Transcode subprocess for {transcode_log} timed out")
            return False

def CreateOrUpdateCacheDirEntry(full_path) -> int:
    global cache
    global cfg

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
    global cache
    global cfg
    global log_prefix_indent

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
        entry_log += f"\n{log_prefix_indent}    Fingerprint: {fingerprint}"
    Log(entry_log_level, entry_log)

    return 1 if is_new_entry else 0

# Return whether a new entry was created, whether a FLAC test was ran, and whether the test passed
def CreateOrUpdateCacheFlacEntry(full_path) -> Tuple[bool, bool, bool]:
    global cache
    global cfg
    global log_prefix_indent

    fingerprint = CalculateFingerprint(full_path)
    relative_path = full_path[len(cfg["library_path"]):]

    is_new_entry = True
    is_modified = False
    for entry in cache.flacs:
        if entry.path == relative_path:
            if fingerprint != entry.fingerprint_on_last_scan:
                entry.fingerprint_on_last_scan = fingerprint
                entry.reencode_ignore_last_fingerprint_change = False
                entry.transcode_ignore_last_fingerprint_change = False
                is_modified = True
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

    test_ran, test_pass, status = ConditionallyRunFlacTest(entry, is_new_entry, is_modified, fingerprint)

    if test_ran and not test_pass:
        entry_log_level = LogLevel.WARN

    entry_log = f"{entry.formatted_path} ({entry_status})"
    if cfg["log_level"] >= LogLevel.TRACE:
        entry_log += f"\n{log_prefix_indent}    Fingerprint: {fingerprint}"
    if test_ran and status:
        entry_log += f"\n{log_prefix_indent}    {status}"
    Log(entry_log_level, entry_log)

    return is_new_entry, test_ran, test_pass

def PrintScanSummary(summary, early_exit=False) -> None:
    global test_specified

    if test_specified:
        summary_log_level = LogLevel.WARN if summary["num_tests_failed"] > 0 else LogLevel.INFO
        num_tests = summary["num_tests_passed"] + summary["num_tests_failed"]
        pass_fail = f": {summary["num_tests_passed"]} passes, {summary["num_tests_failed"]} failures" if num_tests else ""
        test_color = bcolors.WARNING if summary["num_tests_failed"] else bcolors.ENDC
        test_summary = f"\n{test_color}{num_tests} flac tests performed{pass_fail}{bcolors.ENDC}"
    else:
        summary_log_level = LogLevel.INFO
        test_summary = ""

    scan_result = "Scan interrupted:" if early_exit else "Scan complete:"

    Log(summary_log_level, f"{scan_result}\n" \
                           f"{summary["num_dirs"]} dirs ({summary["num_new_dirs"]} new)\n" \
                           f"{summary["num_files"]} files ({summary["num_new_files"]} new)\n" \
                           f"{summary["num_flacs"]} flacs ({summary["num_new_flacs"]} new)" + \
                           test_summary)

    if summary["failed_flac_tests"]:
        PrintFailureList('Failed flac tests:', summary["failed_flac_tests"])

    if early_exit:
        SaveAndQuit()

def IsHiddenFile(path) -> bool:
    global is_windows

    if is_windows:
        return bool(os.stat(path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN)
    else:
        return os.path.basename(path).startswith(".")

def IsHiddenFileOrPath(full_path) -> bool:
    # Treat an unhidden file inside a hidden directory as hidden also. Loop over all parent directories
    path = full_path
    while path and path != "/":
        if IsHiddenFile(path):
            return True
        path = os.path.dirname(path)
    return False

def ScanLibrary() -> None:
    global args
    global cache
    global cfg
    global flag
    global test_specified

    start_time = time()

    scan_test_log = "Scanning and testing library" if test_specified else "Scanning library"
    Log(LogLevel.INFO, f"{scan_test_log} in {cfg["formatted_library_path"]}")

    summary = {
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

    flac_paths = []
    non_flac_paths = []

    for root, dirs, files in os.walk(cfg["library_path"]):
        for dir in dirs:
            full_path = os.path.join(root, dir)
            if not (cfg["ignore_hidden"] and IsHiddenFileOrPath(full_path)):
                summary["num_new_dirs"] += CreateOrUpdateCacheDirEntry(full_path)
                summary["num_dirs"] += 1
            if flag.Exit():
                PrintScanSummary(summary, early_exit=True)
    
        for file in files:
            full_path = os.path.join(root, file)
            if not (cfg["ignore_hidden"] and IsHiddenFileOrPath(full_path)):
                file_extension = file.split(".")[-1]
                if (file_extension == "flac"):
                    flac_paths.append(full_path)
                else:
                    non_flac_paths.append(full_path)
            if flag.Exit():
                PrintScanSummary(summary, early_exit=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        early_exit = False
        future_to_path = {executor.submit(CreateOrUpdateCacheFileEntry, full_path): full_path for full_path in non_flac_paths}
        for future in concurrent.futures.as_completed(future_to_path):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                early_exit = True
                break
        for future in future_to_path:
            if not future.cancelled():
                summary["num_new_files"] += future.result()
                summary["num_files"] += 1

    if early_exit:
        PrintScanSummary(summary, early_exit)

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        early_exit = False
        future_to_path = {executor.submit(CreateOrUpdateCacheFlacEntry, full_path): full_path for full_path in flac_paths}
        for future in concurrent.futures.as_completed(future_to_path):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                early_exit = True
                break
        for future in future_to_path:
            if not future.cancelled():
                full_path = future_to_path[future]
                is_new, test_ran, test_pass = future.result()
                if test_ran and test_pass:
                    summary["num_tests_passed"] += 1
                elif test_ran and not test_pass:
                    summary["num_tests_failed"] += 1
                    summary["failed_flac_tests"].append(full_path)
                summary["num_new_flacs"] += 1 if is_new else 0
                summary["num_flacs"] += 1

    PrintScanSummary(summary, early_exit)

    TimeCommand(start_time, scan_test_log, LogLevel.INFO)

def CheckForOrphanedCache() -> None:
    global cache

    start_time = time()

    num_orphaned_dirs = 0
    num_orphaned_files = 0
    num_orphaned_flacs = 0

    first_file_index = len(cache.dirs)
    first_flac_index = len(cache.dirs) + len(cache.files)

    for index, entry in enumerate(chain(cache.dirs, cache.files, cache.flacs)):
        if not entry.present_in_current_scan:
            Log(LogLevel.WARN, f"Status entry not found in last scan: {entry.formatted_path}")
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
        if args.func == mirror_library:
            Log(LogLevel.INFO, "Orphaned entries and the mirrored files they point to will be removed in the upcoming mirror")
        else:
            Log(LogLevel.INFO, "Orphaned entries and the mirrored files they point to will be removed in the next mirror command")

    TimeCommand(start_time, "Checking for orphaned status entries", LogLevel.INFO)

def ReencodeLibrary() -> None:
    global args
    global cache
    global cfg
    global flac_version

    start_time = time()

    Log(LogLevel.INFO, f"Reencoding FLACs in {cfg["formatted_library_path"]}")

    num_reencoded = 0
    num_failed = 0
    num_interrupted = 0
    num_total = 0

    failed_reencodes = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        early_exit = False
        future_to_entry = {}
        for entry in cache.flacs:
            if entry.present_in_last_scan:
                num_total += 1
                if args.force or \
                   not entry.fingerprint_on_last_reencode or \
                   (args.reencode_on_change and \
                    entry.fingerprint_on_last_scan != entry.fingerprint_on_last_reencode and \
                    not entry.reencode_ignore_last_fingerprint_change) or \
                   (args.reencode_on_update and entry.flac_codec_on_last_reencode != flac_version):
                    future_to_entry[executor.submit(ReencodeFlac, entry)] = entry
        for future in concurrent.futures.as_completed(future_to_entry):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                early_exit = True
                break
        for future in future_to_entry:
            if future.cancelled():
                num_interrupted += 1
            else:
                if future.result():
                    num_reencoded += 1
                else:
                    num_failed += 1
                    failed_reencodes.append(future_to_entry[future].library_path)

    reencode_result = "Library reencode interrupted:" if early_exit else "Library reencode complete:"
    if num_failed > 0:
        summary_log_level = LogLevel.WARN
        reencode_fail = f"\n{bcolors.WARNING}{num_failed} not reencoded due to errors{bcolors.ENDC}"
    else:
        summary_log_level = LogLevel.INFO
        reencode_fail = ""
    if early_exit:
        interrupted_summary = f"\n{num_interrupted} interrupted"
    else:
        interrupted_summary = ""
    Log(summary_log_level, f"{reencode_result}\n" \
                           f"{num_total} total FLACs\n" \
                           f"{num_reencoded} reencoded" + \
                           reencode_fail + \
                           interrupted_summary)

    if failed_reencodes:
        PrintFailureList('Failed reencodes:', failed_reencodes)

    flag.SaveAndQuitIfSignalled()

    TimeCommand(start_time, "Reencoding library", LogLevel.INFO)

def RepadLibrary() -> None:
    global args
    global cache
    global cfg

    start_time = time()

    Log(LogLevel.INFO, f"Re-padding FLACs in {cfg["formatted_library_path"]}")

    num_repadded = 0
    num_padding_ok = 0
    num_failed = 0
    num_interrupted = 0
    num_total = 0

    failed_repads = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        early_exit = False
        future_to_entry = {}
        for entry in cache.flacs:
            if entry.present_in_last_scan:
                num_total += 1
                if args.force or \
                   args.force_repad or \
                   entry.fingerprint_on_last_scan != entry.fingerprint_on_last_repad:
                    future_to_entry[executor.submit(RepadFlac, entry)] = entry
        for future in concurrent.futures.as_completed(future_to_entry):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                early_exit = True
                break
        for future in future_to_entry:
            if future.cancelled():
                num_interrupted += 1
            else:
                repad_attempted, command_successful = future.result()
                if repad_attempted and command_successful:
                    num_repadded += 1
                elif not repad_attempted and command_successful:
                    num_padding_ok += 1
                else:
                    num_failed += 1
                    failed_repads.append(future_to_entry[future].library_path)

    repad_result = "Library repad interrupted:" if early_exit else "Library repad complete:"
    if num_failed > 0:
        summary_log_level = LogLevel.WARN
        repad_fail = f"\n{bcolors.WARNING}{num_failed} not repadded due to errors{bcolors.ENDC}"
    else:
        summary_log_level = LogLevel.INFO
        repad_fail = ""
    if early_exit:
        interrupted_summary = f"\n{num_interrupted} interrupted"
    else:
        interrupted_summary = ""
    Log(summary_log_level, f"{repad_result}\n" \
                           f"{num_total} total FLACs\n" \
                           f"{num_padding_ok} had acceptable padding\n" + \
                           f"{num_repadded} repadded" + \
                           repad_fail + \
                           interrupted_summary)

    if failed_repads:
        PrintFailureList('Failed repads:', failed_repads)

    flag.SaveAndQuitIfSignalled()

    TimeCommand(start_time, "Repadding library", LogLevel.INFO)

def RemoveOrphanedFilesFromPortable() -> None:
    global args
    global cache
    global cfg

    start_time = time()

    Log(LogLevel.INFO, f"Removing orphaned files from {cfg["formatted_output_library_path"]}")

    num_dirs_removed = 0
    num_files_removed = 0
    num_transcodes_removed = 0

    # TODO worth it to remove orphaned file/flac entries from cache as they are deleted by rmtree here?

    for index, entry in enumerate(cache.dirs):
        if not entry.present_in_last_scan:
            deletion_str = f"Delete {entry.formatted_portable_path}"
            if args.dry_run:
                Log(LogLevel.TRACE, f"Dry run: {deletion_str}")
            else:
                if (os.path.isdir(entry.portable_path)):
                    shutil.rmtree(entry.portable_path)
                    Log(LogLevel.TRACE, deletion_str)
                else:
                    Log(LogLevel.INFO, f"Directory {entry.formatted_portable_path} was removed outside of script")
                cache.dirs.pop(index)
            num_dirs_removed += 1
        flag.SaveAndQuitIfSignalled()

    for index, entry in enumerate(cache.files):
        if not entry.present_in_last_scan:
            deletion_str = f"Delete {entry.formatted_portable_path}"
            if args.dry_run:
                Log(LogLevel.TRACE, f"Dry run: {deletion_str}")
            else:
                Log(LogLevel.TRACE, deletion_str)
                Path.unlink(entry.portable_path, missing_ok=True)
                cache.files.pop(index)
            num_files_removed += 1
        flag.SaveAndQuitIfSignalled()

    for index, entry in enumerate(cache.flacs):
        if not entry.present_in_last_scan:
            deletion_str = f"Delete {entry.formatted_portable_path}"
            if args.dry_run:
                Log(LogLevel.TRACE, f"Dry run: {deletion_str}")
            else:
                Log(LogLevel.TRACE, deletion_str)
                Path.unlink(entry.portable_path, missing_ok=True)
                cache.flacs.pop(index)
            num_transcodes_removed += 1
        flag.SaveAndQuitIfSignalled()

    Log(LogLevel.INFO, f"Orphaned file deletion complete:\n" \
                       f"{num_dirs_removed} dirs deleted\n" \
                       f"{num_files_removed} files deleted\n" \
                       f"{num_transcodes_removed} transcodes deleted")

    TimeCommand(start_time, "Removing orphaned files", LogLevel.INFO)

def PrintMirrorAndTranscodeSummary(summary, early_exit=False) -> None:
    global cache
    global flag

    mirror_and_transcode_result = "File mirroring/transcoding interrupted:" if early_exit else "File mirroring/transcoding complete:"

    if summary["num_file_mirrors_failed"] > 0:
        summary_log_level = LogLevel.WARN
        mirror_fail = f"\n{bcolors.WARNING}Files failed to mirror:                   {summary["num_file_mirrors_failed"]}{bcolors.ENDC}"
    else:
        summary_log_level = LogLevel.INFO
        mirror_fail = ""

    if summary["num_flac_transcodes_failed"] > 0:
        summary_log_level = LogLevel.WARN
        transcode_fail = f"\n{bcolors.WARNING}Flacs failed to transcode:                {summary["num_flac_transcodes_failed"]}{bcolors.ENDC}"
    else:
        summary_log_level = LogLevel.INFO
        transcode_fail = ""

    Log(summary_log_level, f"{mirror_and_transcode_result}\n" \
                           f"Directories mirrored (new/total):         {summary["num_dirs_mirrored"]}/{len(cache.dirs)}\n" \
                           f"Files mirrored (new/interrupted/total):   {summary["num_file_mirrors_succeeded"]}/{summary["num_file_mirrors_interrupted"]}/{len(cache.files)}\n" \
                           f"Flacs transcoded (new/interrupted/total): {summary["num_flac_transcodes_succeeded"]}/{summary["num_flac_transcodes_interrupted"]}/{len(cache.flacs)}" + \
                           mirror_fail + \
                           transcode_fail)

    if summary["failed_mirrors"]:
        PrintFailureList('Failed mirrors:', summary["failed_mirrors"])

    if summary["failed_transcodes"]:
        PrintFailureList('Failed transcodes:', summary["failed_transcodes"])

    if early_exit:
        SaveAndQuit()

def MirrorFile(entry) -> bool:
    global cfg

    file_mirror_str = f"{entry.formatted_path} -> {entry.formatted_portable_path}"
    if args.dry_run:
        Log(LogLevel.DEBUG, f"Dry run: {file_mirror_str}")
    else:
        Log(LogLevel.DEBUG, file_mirror_str)
        try:
            Path.unlink(entry.portable_path, missing_ok=True)
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
                        Log(LogLevel.ERROR, f"SHOULD NOT HAPPEN: Invalid file mirror method '{cfg["file_mirror_method"]}'")

            entry.fingerprint_on_last_mirror = entry.fingerprint_on_last_scan
        except OSError as exc:
            Log(LogLevel.WARN, f"Error when mirroring {file_mirror_str}: {exc}")
            return False

    return True

def MirrorLibrary() -> None:
    global args
    global cache
    global cfg
    global opus_version

    start_time = time()

    Log(LogLevel.INFO, f"Mirroring/transcoding files to portable library {cfg["formatted_output_library_path"]}")

    summary = {
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

    early_exit = False

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
            summary["num_dirs_mirrored"] += 1
        if flag.Exit():
            # Do not exit on signals in the loop because this should always be fast; no huge problem to finish this
            early_exit = True

    if early_exit:
        PrintMirrorAndTranscodeSummary(summary, early_exit)

    # Mirror non-flac files
    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["num_threads"]) as executor:
        future_to_entry = {}
        for entry in cache.files:
            if entry.fingerprint_on_last_mirror != entry.fingerprint_on_last_scan or args.force:
                future_to_entry[executor.submit(MirrorFile, entry)] = entry
        for future in concurrent.futures.as_completed(future_to_entry):
            if flag.Exit():
                executor.shutdown(wait=True, cancel_futures=True)
                early_exit = True
                break
        for future in future_to_entry:
            if future.cancelled():
                summary["num_file_mirrors_interrupted"] += 1
            elif future.result():
                summary["num_file_mirrors_succeeded"] += 1
            else:
                summary["num_file_mirrors_failed"] += 1
                summary["failed_mirrors"].append(future_to_entry[future].library_path)

    if early_exit:
        PrintMirrorAndTranscodeSummary(summary, early_exit)

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
                early_exit = True
                break
        for future in future_to_entry:
            if future.cancelled():
                summary["num_flac_transcodes_interrupted"] += 1
            else:
                if future.result():
                    summary["num_flac_transcodes_succeeded"] += 1
                else:
                    summary["num_flac_transcodes_failed"] += 1
                    summary["failed_transcodes"].append(future_to_entry[future].library_path)

    PrintMirrorAndTranscodeSummary(summary, early_exit)

    TimeCommand(start_time, "Mirroring/transcoding files", LogLevel.INFO)

def ListOrphanedEntries() -> None:
    global cache
    global cfg

    start_time = time()

    Log(LogLevel.INFO, f"Listing orphaned files to be removed from {cfg["formatted_output_library_path"]}")

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
    global cache
    global cfg

    start_time = time()

    Log(LogLevel.INFO, f"Listing all scanned files in {cfg["formatted_library_path"]}")

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
    global args
    global cfg

    ReadCache()
    if not args.skip_scan:
        ScanLibrary()
        CheckForOrphanedCache()
    ReencodeLibrary()
    if cfg["check_padding"]:
        RepadLibrary()
    WriteCache()

def mirror_library() -> None:
    global args

    ReadCache()
    if not args.skip_scan:
        ScanLibrary()
        CheckForOrphanedCache()
    RemoveOrphanedFilesFromPortable()
    MirrorLibrary()
    WriteCache()

def convert_playlists() -> None:
    global cfg

    Log(LogLevel.INFO, f"Converting playlists in {cfg["formatted_library_playlist_path"]}")

    # Simpler to recreate all playlists each run; surely no one has enough playlists that this takes time
    if (os.path.isdir(cfg["portable_playlist_path"])):
        shutil.rmtree(cfg["portable_playlist_path"])

    os.makedirs(cfg["portable_playlist_path"])

    for root, dirs, files in os.walk(cfg["library_playlist_path"]):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = file_path[len(cfg["library_playlist_path"]):]
            formatted_path = FormatPath(relative_path, bcolors.OKGREEN)
            if DetectPlaylist(file_path):
                output_path = os.path.join(cfg["portable_playlist_path"], relative_path)
                playlist_convert_str = f"{formatted_path} -> {FormatPath(relative_path, bcolors.OKBLUE)}"
                if args.dry_run:
                    Log(LogLevel.DEBUG, f"Dry run: {playlist_convert_str}")
                else:
                    Log(LogLevel.DEBUG, playlist_convert_str)
                    ConvertPlaylist(file_path, output_path, playlist_convert_str)
            else:
                Log(LogLevel.DEBUG, f"Skipping non-playlist file {formatted_path}")

            flag.SaveAndQuitIfSignalled()

def scan_library() -> None:
    ReadCache()
    ScanLibrary()
    CheckForOrphanedCache()
    WriteCache()

def list_cache():
    global args
    
    ReadCache()
    if args.orphan_only:
        ListOrphanedEntries()
    else:
        ListEntries()

if __name__ == '__main__':
    script_start = time()
    assert sys.version_info >= (3, 10)

    is_windows = system() == "Windows"

    print_lock = Lock()

    log_prefix_indent = "                               "

    args = ParseArgs()

    # Convenience shortcuts for test arguments
    test = hasattr(args, 'test') and args.test
    test_force = hasattr(args, 'test_force') and args.test_force
    retest_on_update = hasattr(args, 'retest_on_update') and args.retest_on_update
    test_specified = test or retest_on_update or test_force

    # ffmpeg has a bad habit of changing stdin attributes when it is terminated
    # ffmpeg is dropped now, but no harm leaving this here in case it's still needed somehow
    original_stdin_attr = termios.tcgetattr(sys.stdin.fileno())

    flag = GracefulExiter()

    cfg = {}
    config_file = "config.yaml"
    config = ReadConfig(config_file)
    if ValidateConfig(config):
        cfg = config
    else:
        Log(LogLevel.ERROR, f"Error(s) found in {config_file}")

    if cfg["num_threads"] == 0:
        if sys.version_info >= (3, 13):
            cfg["num_threads"] = os.process_cpu_count()
        else:
            cfg["num_threads"] = os.cpu_count()
    Log(LogLevel.INFO, f"Using {cfg["num_threads"]} worker threads")

    if args.func == convert_playlists:
        os.makedirs(cfg["portable_playlist_path"], exist_ok=True)

    os.makedirs(cfg["output_library_path"], exist_ok=True)

    flac_version = ""
    opus_version = ""
    CheckDependencies()

    ValidateDependencyConfigArgumentCombinations()

    cache = []

    flag.QuitWithoutSavingIfSignalled()

    library_status_backup_path = cfg["library_status_path"] + ".bak"
    shutil.copy2(cfg["library_status_path"], library_status_backup_path)
    Log(LogLevel.INFO, f"Saved backup of current library status file at {library_status_backup_path}")

    args.func()

    TimeCommand(script_start, "MusicMirror", LogLevel.INFO)
