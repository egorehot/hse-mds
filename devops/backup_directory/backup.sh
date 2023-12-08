#!/bin/bash

function display_help() {
    echo "Usage: $0 -d <directory> -c <compression> -o <output_file>"
    echo "Options:"
    echo "  -d, --directory      Directory to backup"
    echo "  -c, --compression    Compression algorithm (none, gzip, bzip2, xz, lzma)"
    echo "  -o, --output         Output file name"
    echo "  -h, --help           Display this help message"
    exit 1
}

function handle_directory() {
    DIRECTORY="$1"
}

function handle_compression() {
    COMPRESSION="$1"
}

function handle_output() {
    OUTPUT_FILE="$1"
}

function create_backup() {
    tar_command="tar -c $DIRECTORY"

    case $COMPRESSION in
        "none") ;;
        "gzip")  tar_command="$tar_command -z" ;;
        "bzip2") tar_command="$tar_command -j" ;;
        "xz")    tar_command="$tar_command -J" ;;
	"lzma")  tar_command="$tar_command --lzma" ;;
        *)
            echo "Error: Unknown compression algorithm '$COMPRESSION'"
            exit 1
            ;;
    esac
    
    $tar_command | openssl enc -aes-256-cbc -out "$OUTPUT_FILE" 2>> error.log
}

# Main script
if [ $# -eq 0 ]; then
    display_help
fi

while [ $# -gt 0 ]; do
    case "$1" in
        -d|--directory)
            shift
            handle_directory "$1"
            ;;
        -c|--compression)
            shift
            handle_compression "$1"
            ;;
        -o|--output)
            shift
            handle_output "$1"
            ;;
        -h|--help)
            display_help
            ;;
        *)
            echo "Error: Unknown option $1"
            exit 1
            ;;
    esac
    shift
done

if [ -z "$DIRECTORY" ] || [ -z "$COMPRESSION" ] || [ -z "$OUTPUT_FILE" ]; then
    echo "Error: Missing mandatory parameters"
    display_help
fi

exec 1>/dev/null

create_backup

