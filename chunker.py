#!/usr/bin/env python3

import os
import sys
import time
import argparse
import json

from os import PathLike
from typing import Optional, List, Tuple

# encoder

class _Chunker:
    def __init__(self, outdir: PathLike, mb_per_chunk: int):
        self.__bytes_per_chunk = mb_per_chunk * 1024 * 1024
        self.__outdir = outdir
        self.__chunk_index = -1
        self.__byte_index = 0
        self.__file_positions = {}
        self.__can_write = True
        self.__chunk = None
        self.__switch_to_next_chunk() # will initialize other chunk vars
    
    @property
    def outdir(self):
        return self.__outdir
    
    @property
    def chunk_index(self):
        return self.__chunk_index
    
    @property
    def file_index(self):
        return self.__file_index
    
    def __switch_to_next_chunk(self):
        if self.__chunk is not None:
            print(f"Filled chunk {self.__chunk_index}")
            self.__chunk.close()
        self.__chunk_index += 1
        next_name = os.path.join(self.__outdir, f"chunk-{self.__chunk_index}")
        self.__this_chunk_bytes = 0
        self.__chunk = open(next_name, 'wb')
    
    def read_from_path(self, path, name):
        if not self.__can_write:
            raise Exception(f"Already closed! Cannot chunk more files.")
        
        if name in self.__file_positions:
            raise KeyError(f"File name '{name}' already exists in this chunker!")
        self.__file_positions[name] = self.__byte_index
        
        n = self.__bytes_per_chunk - self.__this_chunk_bytes
        with open(path, 'rb') as file:
            while (b:=file.read(n)):
                # write what we read to the current chunk
                self.__this_chunk_bytes += len(b)
                self.__byte_index += len(b)
                self.__chunk.write(b)
                # get a new chunk if this one was filled
                n = self.__bytes_per_chunk - self.__this_chunk_bytes
                if not n:
                    self.__switch_to_next_chunk()
                    n = self.__bytes_per_chunk
    
    def close(self):
        self.__can_write = False
        self.__chunk.close()
        print(f"Filled chunk {self.__chunk_index}")
    
    def write_header_file(self):
        if self.__can_write:
            raise Exception(f"Not closed! Cannot write headers.")
        header_path = os.path.join(self.__outdir, 'header')
        with open(header_path, 'w') as file:
            json.dump({
                    'bytes_per_chunk': self.__bytes_per_chunk,
                    'positions': self.__file_positions,
                    'total_bytes': self.__byte_index
                }, file)
    

def collapse_tree(dir: PathLike) -> Tuple[str, List[str]]:
    dir = os.path.abspath(dir)
    if not os.path.exists(dir): raise FileNotFoundError(dir)
    # short circuit - this isn't a dir
    if not os.path.isdir(dir):
        parts = os.path.split(dir)
        return parts[0], [parts[1]]
    # walk the tree!
    flattened = []
    for (root, dirs, files) in os.walk(dir):
        for file in files:
            full = os.path.join(root, file)
            flattened.append(full.removeprefix(dir+'/'))
    return dir, flattened

def encode(infile: PathLike, outdir: PathLike, mb_per_chunk: int) -> bool:
    # check out dir
    if not isinstance(outdir, str):
        raise TypeError(f"Output directory '{outdir}' is not a str (type '{type(outdir)}')")
    outdir = os.path.abspath(outdir)
    if not os.path.exists(outdir):
        raise FileNotFoundError(f"Output directory '{outdir}' not found")
    if not os.path.isdir(outdir):
        raise NotADirectoryError(f"Output directory '{outdir}' is not a directory")
    # check in file
    if not isinstance(infile, str):
        raise TypeError(f"Input file '{infile}' is not a str (type '{type(outdir)}')")
    infile = os.path.abspath(infile)
    if not os.path.exists(infile):
        raise FileNotFoundError(f"Input file '{infile}' not found")
    
    base,infiles = collapse_tree(infile)
    
    # chunk the files!
    time_start = time.time()
    chunker = _Chunker(outdir, mb_per_chunk)
    errored = False
    try:
        for name in infiles:
            path = os.path.join(base, name)
            chunker.read_from_path(path, name)
            print(f"Chunked '{path}' as '{name}'")
    
    except Exception as e:
        errored = True
        print("------")
        print(type(e), e)
    
    finally:
        chunker.close()
        print("------")
        if not errored:
            chunker.write_header_file()
            delta = time.time() - time_start
            print(f"Done, took {delta:.2f} seconds.")
            print(f"Encoded {len(infiles)} files into {chunker.chunk_index} chunks.")
        else:
            print("Error chunking files!")
        return errored



# decoder

class _Dechunker:
    def __init__(self, indir: PathLike, mb_per_chunk: int):
        self.__bytes_per_chunk = mb_per_chunk * 1024 * 1024
        self.__indir = indir
        self.__chunk_index = -1
        self.__can_read = True
        self.__chunk = None
        self.__switch_to_next_chunk() # will initialize other chunk vars
    
    @property
    def indir(self):
        return self.__indir
    
    @property
    def chunk_index(self):
        return self.__chunk_index
    
    def __switch_to_next_chunk(self):
        if self.__chunk is not None:
            print(f"Drained chunk {self.__chunk_index}")
            self.__chunk.close()
        self.__chunk_index += 1
        next_name = os.path.join(self.__indir, f"chunk-{self.__chunk_index}")
        self.__chunk = open(next_name, 'rb')
    
    def write_n_bytes(self, n, outpath):
        if not self.__can_read:
            raise Exception(f"Already closed! Cannot chunk more files.")
        
        with open(outpath, 'wb') as file:
            while n > 0:
                b = self.__chunk.read(n)
                if not b: self.__switch_to_next_chunk()
                else:
                    file.write(b)
                    n -= len(b)
        
    #     if name in self.__file_positions:
    #         raise KeyError(f"File name '{name}' already exists in this chunker!")
    #     self.__file_positions[name] = self.__byte_index
        
    #     n = self.__bytes_per_chunk - self.__this_chunk_bytes
    #     with open(path, 'rb') as file:
    #         while (b:=file.read(n)):
    #             # write what we read to the current chunk
    #             self.__this_chunk_bytes += len(b)
    #             self.__byte_index += len(b)
    #             self.__chunk.write(b)
    #             # get a new chunk if this one was filled
    #             n = self.__bytes_per_chunk - self.__this_chunk_bytes
    #             if not n:
    #                 self.__switch_to_next_chunk()
    #                 n = self.__bytes_per_chunk
    
    def close(self):
        self.__can_read = False
        self.__chunk.close()
        print(f"Drained chunk {self.__chunk_index}")

def decode(indir: PathLike, outdir: PathLike) -> bool:
    # check out dir
    if not isinstance(outdir, str):
        raise TypeError(f"Output directory '{outdir}' is not a str (type '{type(outdir)}')")
    outdir = os.path.abspath(outdir)
    if not os.path.exists(outdir):
        raise FileNotFoundError(f"Output directory '{outdir}' not found")
    if not os.path.isdir(outdir):
        raise NotADirectoryError(f"Output directory '{outdir}' is not a directory")
    # check in dir
    if not isinstance(indir, str):
        raise TypeError(f"Input directory '{indir}' is not a str (type '{type(indir)}')")
    indir = os.path.abspath(indir)
    if not os.path.exists(indir):
        raise FileNotFoundError(f"Input directory '{indir}' not found")
    if not os.path.isdir(indir):
        raise NotADirectoryError(f"Input directory '{indir}' is not a directory")
    
    # look for header
    header_path = os.path.join(indir, 'header')
    if not os.path.exists(header_path):
        raise FileNotFoundError(f"No header file found in input directory '{indir}'")
    try:
        with open(header_path, 'r') as file:
            data = json.load(file)
            mb_per_chunk = data['bytes_per_chunk'] // (1024*1024)
            file_positions = data['positions']
            total_bytes = data['total_bytes']
    except:
        raise ValueError(f"Header file corrupted or invalid")
    
    # make sure positions list is sorted
    file_positions = list(sorted(file_positions.items(), key=lambda i: i[1]))
    
    # start reading!
    time_start = time.time()
    dechunker = _Dechunker(indir, mb_per_chunk)
    errored = False
    try:
        for i in range(len(file_positions)):
            # calculate this file's length
            this = file_positions[i]
            if i == len(file_positions)-1:
                ends = total_bytes
            else:
                ends = file_positions[i+1][1]
            length = ends - this[1]
            path = os.path.join(outdir, this[0])
            # ... and write it out
            dechunker.write_n_bytes(length, path)
            print(f"Wrote '{this[0]}' as '{path}'")
    
    except Exception as e:
        errored = True
        print("------")
        print(type(e), e, sep='\n')
    
    finally:
        print("------")
        if not errored:
            delta = time.time() - time_start
            print(f"Done, took {delta:.2f} seconds.")
            print(f"Decoded {len(file_positions)} files.")
        else:
            print("Error dechunking files! Written files may be corrupt.")
        return errored



# gui

def do_gui():
    print("Launching GUI...")
    print("TODO this is not functional yet. Exiting.")
    # TODO



# command line parsing

def do_cli():
    parser = argparse.ArgumentParser(
        description="Splits large files into groups of 25mb to get around discord's upload limit.")
    subparsers = parser.add_subparsers(title='actions', required=True)

    encode_parser = subparsers.add_parser('encode', description="Encode file into 25mb chunks")
    encode_parser.set_defaults(func=encode)
    encode_parser.add_argument('infile', help="The file to encode")
    encode_parser.add_argument('outdir', help="The directory to write the chunk files to")
    encode_parser.add_argument('-s', '--chunk-size', required=False, default=25, type=int, dest='mb_per_chunk',
                               help="Size, in MB, of each chunk. For most users this is 25MB, but Nitro users or popular servers can upload larger files.")
   #encode_parser.add_argument('-c', '--compress', action='store_true')

    decode_parser = subparsers.add_parser('decode', description="Decode chunk files to their original file")
    decode_parser.set_defaults(func=decode)
    decode_parser.add_argument('indir', help="The directory to read as a set of chunk files. Defaults to current working directory.")
    decode_parser.add_argument('-o', '--outdir', required=False, default=os.getcwd(),
                               help="Write the decoded file to this directory", )

    args = vars(parser.parse_args())
    func = args.pop('func')
    func(**args)



# decide if cli or gui

if __name__ == '__main__':
    if len(sys.argv) <= 1:  do_gui()
    else:                   do_cli()
