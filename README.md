msgpack-matlab
==============

MessagePack for Matlab based on msgpack-c (https://github.com/msgpack/msgpack-c)

install: mex -O msgpack.cc

API

Packer:
msg = msgpack('pack', var1, var2, ...)

Unpacker: 
obj = msgpack('unpack', msg) 
  return numericArray or charArray or LogicalArray if data are numeric
  otherwise return Cell or Struct
  
Streaming unpacker:
objs = msgpack('unpacker', msg)
  return Cell containing numericArray, charArray, Cell or Struct

#Issue

  1. Since Matlab string is two-bytes (UTF16), 
    >> msgpack('unpack', msgpack('pack', 'hello')) returns
    >> h e l l o

    For correct string size, use
    >> msg = msgpack('pack', uint8('hello'))
    >> char(msgpack('unpack', msg))'
    >> hello
