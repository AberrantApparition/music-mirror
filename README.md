# MusicMirror

Maintain a mirror image of a FLAC music library, transcoded to Opus.

A fingerprint of each file is saved in `fingerprints.yaml`, allowing for incremental processing of only updated or new files.

## Features
- Reencode all FLAC files in library with max compression and configurable padding
- Test decoding all FLACs to check for errors
- Mirror entire library with all FLAC files transcoded to Opus
- Mirror non-FLAC files by copy, symlink, or hard-link
- Fingerprints either in the form of either file modification times or file hashes
- `fingerprints.yaml` is human-readable and editable, allowing for manual management if necessary
- Multithreaded scanning, reencoding, copying, and transcoding
- Optionally reencode and re-mirror when FLAC or Opus codecs are updated
- Mirror `M3U` and `M3U8` playlists

## Limitations
- Does not scan for changes in the mirrored library. If you accidentally delete a mirrored file you have to manually modify that fingerprint entry in `fingerprints.yaml` to make MusicMirror re-mirror it (or use `-f` to re-mirror everything)
- Does not attempt to "move" entries in `fingerprints.yaml` if you rename a file or directory. It will just naively detect one deleted file and one new file
- Only supports FLAC and Opus
- No extra processing is done on FLAC tags once transcoded to Opus. For example, ReplayGain tags will not be updated to R128_*_GAIN tags. Use a tool like `rsgain` to update ReplayGain tags after running MusicMirror
- Not tested on Windows or Mac (though it is designed to work cross-platform)
- Padding is only adjusted with the `flac` codec, not `ffmpeg`
- Will reencode on any change to flac file, even if a reencode is not necessary. This will be fixed at some point

## Requirements
- Python 3.10 or higher
- `PyYaml`
- To encode FLAC: `flac`, or `ffmpeg` with the `flac` codec
- To test FLAC files: `flac` or `ffprobe`
- To encode Opus: `opusenc`, or `ffmpeg` with either the `libopus` or `opus` codec

## Setup
1. Install PyYaml:
   ```bash
   pip install PyYaml
   ```
2. Adjust settings in `config.yaml`. At a minimum set the locations of the source library and mirrored library.

## Usage
Scan library to build or update fingerprints file:
   ```bash
   ./musicmirror.py scan
   ```
Scan library and test decoding flac files:
   ```bash
   ./musicmirror.py scan -t
   ```
List scanned library items:
   ```bash
   ./musicmirror.py list
   ```
Reencode library:
   ```bash
   ./musicmirror.py reencode
   ```
Mirror library:
   ```bash
   ./musicmirror.py mirror
   ```
Mirror playlists, to reference Opus tracks:
   ```bash
   ./musicmirror.py convert_playlists
   ```

You can interrupt the script with `ctrl-c`. The script will let threads finish their current task (though `ffmpeg` threads will terminate immediately) and save current progress in `fingerprints.yaml`

### Control when actions are re-done

`scan -t` will only test flac files on first run or when their fingerprints change.  `scan -u` will re-test if the testing tool has updated or changed since the last test. `scan -T` will test all flac files.

By default `reencode` and `mirror` will only re-run on a file if its fingerprint changes. `reencode` and `mirror` share options. `-u` will additionally re-run on files if the relevant codec has updated or been changed since last run. `-f` will re-run on every file. `-k` will skip the scan that normally happens at the start of each `reencode` or `mirror`. If you use `-k` be careful to ensure that the source library is not modified between `scan` and `reencode`/`mirror`.

`reencode`, `mirror`, and `convert_playlists` all have a `--dry-run` option that only shows what changes will happen.
