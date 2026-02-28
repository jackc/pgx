#!/bin/bash
set -e

# Run any additional setup scripts included in the shared/devcontainer directory. This is to allow for per developer or
# per-environment customizations. These scripts are not checked into source control.
if [ -x "../shared/devcontainer/install" ]; then
  ../shared/devcontainer/install
fi

# Create a symlink to the shared .scratch directory for temporary files if it exists.
if [ -x "../shared/.scratch" ]; then
  if [ ! -e .scratch ] && [ ! -L .scratch ]; then
    ln -s ../shared/.scratch
  fi
fi
