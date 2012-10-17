wolfebin
========

Quick-and-easy file sharing.


Using the client
----------------

Put `wolfebin` in your PATH. The other files here are not necessary.

To configure for first-time use:

```
wolfebin init
```

To share/upload some files:

```
wolfebin put files...
```

To download the shared files:

```
wolfebin get
```

Any subsequent uploads will overwrite the previous one. To upload something
meant to stick around for a long time, you can use a ''key'' to identify it.

```
wolfebin put -k taxes IncomeTaxes.pdf
wolfebin get taxes
```

To list the keys that exist:

```
wolfebin ls
```

To delete a key:

```
wolfebin delete taxes
```

See `wolfebin help` for more details.


Setting up a server
-------------------

The server is only tested to work in linux. You must have both
`wolfebin_server.py` and `wolfebin` together for the server to work.

```
mkdir ~/wolfebin_server
cd ~/wolfebin_server
/path/to/wolfebin_server.py
```

This will create a config file and the database below `~/wolfebin_server`.

As with all servers, you must know your public I.P. address or setup a dns
redirect. Then you probably need to configure port forwarding on your router.
