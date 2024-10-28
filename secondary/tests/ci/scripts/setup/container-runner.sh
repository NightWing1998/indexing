#!/bin/bash

echo "container runner started"

echo '#include <unistd.h>\nint main(){for(;;)pause();}' > pause.c
gcc -o pause pause.c

[ -z "$CONTAINER_INIT_SCRIPT" ] && export CONTAINER_INIT_SCRIPT="$ciscripts_dir/secondary/tests/ci/scripts/build"

bash $CONTAINER_INIT_SCRIPT &
./pause
