#!/bin/bash

docker build -t starwitorg/sae-velocity-analyzer:$(poetry version --short) .
