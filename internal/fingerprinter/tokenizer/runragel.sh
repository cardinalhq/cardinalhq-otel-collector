#!/bin/sh

set -x

ragel -Z -G2 tokenizer.rl -o /dev/stdout | sed '1 s,^.*$,// GENERATED CODE.  DO NOT EDIT.,' > tokenizer.go
