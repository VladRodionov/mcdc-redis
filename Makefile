# mcdc-redis/Makefile
# Cross-platform build for macOS/Linux. Produces:
#   - build/mcdc.so        (release)
#   - build-debug/mcdc.so  (debug)

# --- Toolchain ---------------------------------------------------------------
CC      ?= cc
AR      ?= ar
PREFIX  ?= /usr/local
UNAME_S := $(shell uname -s)

# --- Build type: release (default) or debug ----------------------------------
BUILD_TYPE ?= release

ifeq ($(BUILD_TYPE),debug)
  OPT        = -O0 -gdwarf-4
  DBG        = -g
  DEFS_EXTRA = -DMCDC_DEBUG
  BUILD_DIR  = build-debug
else
  OPT       ?= -O3
  DBG        =
  DEFS_EXTRA =
  BUILD_DIR  = build
endif

# --- OS-specific shared lib settings ----------------------------------------
ifeq ($(UNAME_S),Darwin)
  SHLIB_EXT = so
  PIC       = -fPIC
  DEFS_OS   = -D_DARWIN_C_SOURCE
  # Build a Mach-O bundle suitable for dlopen (Redis modules)
  SOFLAGS   = -bundle -Wl,-undefined,dynamic_lookup
else
  SHLIB_EXT = so
  PIC       = -fPIC
  DEFS_OS   = -D_GNU_SOURCE
  SOFLAGS   = -shared
endif

# --- Zstd via pkg-config (fallback to -lzstd) --------------------------------
ZSTD_CFLAGS := $(shell pkg-config --cflags libzstd 2>/dev/null)
ZSTD_LIBS   := $(shell pkg-config --libs   libzstd 2>/dev/null)

# Fallback for macOS + Homebrew when pkg-config isn't in PATH (Xcode case)
ifeq ($(UNAME_S),Darwin)
  ifeq ($(strip $(ZSTD_CFLAGS)),)
    # Adjust these paths if your zstd is elsewhere
    ZSTD_CFLAGS = -I/opt/homebrew/opt/zstd/include
  endif
  ifeq ($(strip $(ZSTD_LIBS)),)
    ZSTD_LIBS = -L/opt/homebrew/opt/zstd/lib -lzstd
  endif
else
  ifeq ($(strip $(ZSTD_LIBS)),)
    ZSTD_LIBS = -lzstd
  endif
endif

# --- Common flags ------------------------------------------------------------
CSTD    = -std=c11
WARN    = -Wall -Wextra -Wpedantic -Wshadow -Wpointer-arith -Wcast-align -Wstrict-prototypes
DEFS    = $(DEFS_OS) $(DEFS_EXTRA)
INC     = -Iinclude -Ideps/redis

CFLAGS  ?=
CFLAGS  += $(CSTD) $(WARN) $(OPT) $(DBG) $(PIC) $(DEFS) $(INC) $(ZSTD_CFLAGS)
LDFLAGS ?=
LIBS    = $(ZSTD_LIBS)

# --- Sources -----------------------------------------------------------------
SRC = \
  src/mcdc_module.c \
  src/mcdc_compression.c \
  src/mcdc_config.c \
  src/mcdc_dict.c \
  src/mcdc_dict_pool.c \
  src/mcdc_eff_atomic.c \
  src/mcdc_gc.c \
  src/mcdc_incompressible.c \
  src/mcdc_rdb.c \
  src/mcdc_replication.c \
  src/mcdc_sampling.c \
  src/mcdc_stats.c \
  src/mcdc_utils.c \
  src/mcdc_admin_cmd.c

OBJ    = $(patsubst src/%.c,$(BUILD_DIR)/%.o,$(SRC))
TARGET = $(BUILD_DIR)/mcdc.$(SHLIB_EXT)

# --- Default targets ---------------------------------------------------------
all: $(TARGET)

debug:
	$(MAKE) clean
	$(MAKE) BUILD_TYPE=debug

$(BUILD_DIR):
	@mkdir -p $@

$(BUILD_DIR)/%.o: src/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(TARGET): $(OBJ)
	$(CC) $(SOFLAGS) -o $@ $(OBJ) $(LDFLAGS) $(LIBS)

# --- Utility -----------------------------------------------------------------
clean:
	rm -rf build build-debug

install: all
	mkdir -p $(DESTDIR)$(PREFIX)/lib
	cp $(TARGET) $(DESTDIR)$(PREFIX)/lib/

print-%:
	@echo '$*=$($*)'

.PHONY: all debug clean install
