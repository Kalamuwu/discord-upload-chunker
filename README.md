# discord-upload-chunker

A simple script to chunk and de-chunk large files to fit within discord's 25mb upload limit



## Installation

1. Install python 3, if not already installed
2. Place this script somewhere, like `~/.local/bin` on Linux or `C:\Users\...\Documents` on Windows
3. Done!



## File Standard

This program operates with the following files:

> `header`: Defines the name, size, length, and byte index of the decoded files
> 
> `chunk-NN`: The packaged data itself

For encoding, these files will be generated. For decoding, place these files in a new empty folder, and call `decode` mode on that folder.



## Chunking

Call the script in `encode` mode:

```bash
python3 chunker.py encode <input_file_or_dir> <output_dir>
```

This will chunk together the given `input_file`, or, if given a directory, all the files within `input_dir`. The output files will be placed in `output_dir`.



## Uploading/Downloading

On the discord side of things, upload all the chunk files that were produced, as well as the header file.

To reproduce the files, download all the chunk files and the header file and place those into a new, empty folder.



## De-chunking

Call the script in `decode` mode:

```bash
python3 chunker.py decode <chunk_dir>
```

This will de-chunk the chunk and header files found in `chunk_dir`.

To specify the output folder of the new files, use the `-o` flag:

```bash
python3 chunker.py decode <chunk_dir> -o <output_dir>
```

This will place the decoded files in `output_dir`, instead of the current working directory.



## WIP

- GUI eventually
- Zipping/compression?
