AC_INIT([mcdc-redis], [0.1.0], [issues@yourdomain.example], [mcdc-redis])
AM_INIT_AUTOMAKE([foreign subdir-objects dist-xz no-dist-gzip])
AC_CONFIG_SRCDIR([src/module.c])
AC_CONFIG_HEADERS([include/mcdc_config.h])
AC_PROG_CC
AM_PROG_AR
LT_INIT([disable-static])

# C flags
CFLAGS="$CFLAGS -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wpointer-arith -Wcast-align -O3 -fPIC -D_GNU_SOURCE"

# Headers
AC_CHECK_HEADERS([stdlib.h stdint.h string.h errno.h])

# Redis Module header (vendored)
AC_CHECK_FILE([deps/redis/redismodule.h], [], [AC_MSG_ERROR([deps/redis/redismodule.h not found])])

# libzstd via pkg-config (required)
PKG_PROG_PKG_CONFIG
PKG_CHECK_MODULES([ZSTD], [libzstd >= 1.5.0])

AC_SUBST([ZSTD_CFLAGS])
AC_SUBST([ZSTD_LIBS])

# Outputs
AC_CONFIG_FILES([
  Makefile
])
AC_OUTPUT
