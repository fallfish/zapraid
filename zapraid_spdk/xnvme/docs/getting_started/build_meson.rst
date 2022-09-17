.. code-block:: bash

  cd xnvme

  # configure xNVMe and build dependencies (fio, liburing, and SPDK)
  meson setup builddir
  cd builddir

  # build xNVMe
  meson compile

  # install xNVMe
  meson install

  # uninstall xNVMe
  # meson --internal uninstall
