#!/bin/bash
# Generate SSH key pair for Jepsen control -> DB node communication
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
SECRET_DIR="$DIR/secret"

mkdir -p "$SECRET_DIR"

if [ ! -f "$SECRET_DIR/id_rsa" ]; then
  echo "Generating SSH key pair..."
  ssh-keygen -t rsa -b 2048 -f "$SECRET_DIR/id_rsa" -N "" -q
  echo "SSH keys generated in $SECRET_DIR/"
else
  echo "SSH keys already exist in $SECRET_DIR/"
fi
