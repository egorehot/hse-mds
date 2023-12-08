# Backup directory script
## Task description:
In this task you need to create a backup script. The script should accept three parameters:
- the directory to backup;
- the compression algorithm to use (none, gzip, bzip, etc; see tar for details);
- the output file name.

The resulting backup archive should be encrypted (see openssl enc for details).
All output except errors should be suspended, all errors should be written to the error.log file instead of stderr.
Add -h/—help arg to your script and Readme.md in repo with user description for +1 to grade.
BASH scripts allow you to use functions as in other high-level programming languages; break the functionality of the script into such functions, calling each of them with its own command line argument (./backup.sh -c ... for compression, for example) for +1 to grade.

## Solution
The script is implemented in `backup.sh` file. It has 5 functions:
- `display_help` - displays help message when `backup.sh -h` was called or any misusage of other arguments.
- three functions that handle passed arguments `handle_...`.
- the main function `create_backup` which imlpements the main logic.

The script requires 3 parameters:
- directory to backup must be passed using `-d` or `--directory` argument.
- compression algorithm (none, gzip, bzip2, xz, lzma) must be passed using `-c` or `--compression` parameters.
- the output file name - `-o` or `--output`

After calling script with propper arguments it is necessary to pass encryption password to encrypt the archive using `openssl enc`.
