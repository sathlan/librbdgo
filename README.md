librbdgo
----

The `librbdgo` package provides support for a very small subset of the
[ceph rbd library](http://ceph.com/docs/next/rbd/librbdpy/).

It's based on the code of the ceph python binding.

usage and examples
------------------

See [godoc.org/github.com/sathlan/librbdgo](http://godoc.org/github.com/sathlan/librbdgo) for examples and usage.

It needs a rados library that implements the `IoCtxCreateDestroyer` interface.
[libradosgo](https://github.com/sathlan/libradosgo) is such a library.

roadmap
-------

 * Implements more of the rbd library.


contributing
------------

Features, Issues, and Pull Requests are always welcome.
