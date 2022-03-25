#!/bin/bash

python -m virtualenv env
. ./env/bin/activate || . ./env/Scripts/activate
pip install codecov
python -m codecov
