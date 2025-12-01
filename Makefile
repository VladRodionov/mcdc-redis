# mcdc-redis/Makefile
# Cross-platform build for macOS/Linux. Produces:
#   - build/mcdc.so        (release)
#   - build-debug/mcdc.so  (debug)
#   - build-tests/*        (test binaries)

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
LIBS    = $(ZSTD_LIBS) -lm

# --- Sources: Redis module (core + glue) -------------------------------------
SRC = \
  src/mcdc_module.c \
  src/mcdc_compression.c \
  src/mcdc_config.c \
  src/mcdc_dict.c \
  src/mcdc_dict_pool.c \
  src/mcdc_eff_atomic.c \
  src/mcdc_gc.c \
  src/mcdc_incompressible.c \
  src/mcdc_sampling.c \
  src/mcdc_stats.c \
  src/mcdc_utils.c \
  src/mcdc_admin_cmd.c \
  src/mcdc_string_cmd.c \
  src/mcdc_cmd_filter.c \
  src/mcdc_role.c \
  src/mcdc_module_utils.c \
  src/mcdc_string_unsupported_cmd.c \
  src/mcdc_mget_async2.c \
  src/mcdc_thread_pool.c \
  src/mcdc_mset_async2.c \
  src/mcdc_hash_cmd.c \
  src/mcdc_hash_async.c \
  src/mcdc_log.c \
  src/mcdc_module_log.c \
  src/mcdc_env.c \
  src/mcdc_env_redis.c \
  src/mcdc_dict_load_async.c

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

# --- C test suite (smoke tests) ----------------------------------------------
TEST_DIR        := test/smoke
TEST_BUILD_DIR  := build-tests

# Test sources (each .c becomes its own binary)
TEST_SRC  := $(wildcard $(TEST_DIR)/*.c)
TEST_OBJ  := $(patsubst $(TEST_DIR)/%.c,$(TEST_BUILD_DIR)/%.o,$(TEST_SRC))
TEST_BINS := $(patsubst $(TEST_DIR)/%.c,$(TEST_BUILD_DIR)/%,$(TEST_SRC))

# MC/DC core sources for tests (no Redis glue)
CORE_SRC := \
  src/mcdc_module.c \
  src/mcdc_compression.c \
  src/mcdc_config.c \
  src/mcdc_dict.c \
  src/mcdc_dict_pool.c \
  src/mcdc_eff_atomic.c \
  src/mcdc_gc.c \
  src/mcdc_incompressible.c \
  src/mcdc_sampling.c \
  src/mcdc_stats.c \
  src/mcdc_utils.c \
  src/mcdc_admin_cmd.c \
  src/mcdc_string_cmd.c \
  src/mcdc_cmd_filter.c \
  src/mcdc_role.c \
  src/mcdc_module_utils.c \
  src/mcdc_string_unsupported_cmd.c \
  src/mcdc_mget_async2.c \
  src/mcdc_thread_pool.c \
  src/mcdc_mset_async2.c \
  src/mcdc_hash_cmd.c \
  src/mcdc_hash_async.c \
  src/mcdc_log.c \
  src/mcdc_module_log.c \
  src/mcdc_env.c \
  src/mcdc_env_redis.c \
  src/mcdc_dict_load_async.c

CORE_OBJ := $(patsubst src/%.c,$(TEST_BUILD_DIR)/%.o,$(CORE_SRC))

$(TEST_BUILD_DIR):
	@mkdir -p $@

# Build test object files
$(TEST_BUILD_DIR)/%.o: $(TEST_DIR)/%.c | $(TEST_BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Build MC/DC core object files for tests
$(TEST_BUILD_DIR)/%.o: src/%.c | $(TEST_BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Link each test binary with MC/DC core
$(TEST_BUILD_DIR)/%: $(TEST_BUILD_DIR)/%.o $(CORE_OBJ)
	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)

.PHONY: test
test: $(TEST_BINS)
	@if [ -z "$(TEST_SRC)" ]; then \
	  echo "No smoke tests found in $(TEST_DIR)"; \
	  exit 1; \
	fi ; \
	echo "Running smoke tests..." ; \
	for t in $(TEST_BINS); do \
	  echo "==> $$t"; \
	  "$$t"; \
	done ; \
	echo "All smoke tests passed."

###############################################################################
# Python Integration Tests (pytest + virtualenv)
###############################################################################

PYTHON      ?= python3
VENV_DIR    ?= .venv
ITEST_DIR   ?= test/integration
REQ_FILE    ?= $(ITEST_DIR)/requirements.txt

PYTHON_BIN  := $(VENV_DIR)/bin/python
PIP_BIN     := $(VENV_DIR)/bin/pip
PYTEST_BIN  := $(VENV_DIR)/bin/pytest

# Create / update virtualenv and install dependencies
.PHONY: venv
venv:
	@if [ ! -d "$(VENV_DIR)" ]; then \
	  echo "Creating Python virtualenv in $(VENV_DIR)..."; \
	  $(PYTHON) -m venv "$(VENV_DIR)" || { \
	    echo "ERROR: failed to create venv. On Ubuntu, install 'python3-venv'."; \
	    exit 1; \
	  }; \
	fi; \
	if [ ! -x "$(PIP_BIN)" ]; then \
	  echo "Bootstrapping pip in venv using ensurepip..."; \
	  "$(PYTHON_BIN)" -m ensurepip --upgrade || true; \
	fi; \
	echo "Upgrading pip..."; \
	"$(PYTHON_BIN)" -m pip install --upgrade pip >/dev/null; \
	if [ -f "$(REQ_FILE)" ]; then \
	  echo "Installing Python dependencies from $(REQ_FILE)..."; \
	  "$(PYTHON_BIN)" -m pip install -r "$(REQ_FILE)"; \
	else \
	  echo "WARNING: $(REQ_FILE) not found; skipping dependency install."; \
	fi
# Integration tests: build module, ensure venv+deps, run pytest
.PHONY: itest
itest: $(TARGET) venv
	@if [ ! -x "$(PYTEST_BIN)" ]; then \
	  echo "ERROR: pytest not installed in $(VENV_DIR)."; \
	  exit 1; \
	fi; \
	if [ ! -d "$(ITEST_DIR)" ]; then \
	  echo "ERROR: integration test directory '$(ITEST_DIR)' not found."; \
	  exit 1; \
	fi; \
	FILES=""; \
	if [ -f "$(ITEST_DIR)/test_strings.py" ]; then \
	  FILES="$$FILES $(ITEST_DIR)/test_strings.py"; \
	fi; \
	if [ -f "$(ITEST_DIR)/test_hashes.py" ]; then \
	  FILES="$$FILES $(ITEST_DIR)/test_hashes.py"; \
	fi; \
	if [ -z "$$FILES" ]; then \
	  echo "ERROR: no integration test files (test_strings.py / test_hashes.py) found in $(ITEST_DIR)"; \
	  exit 1; \
	fi; \
	echo "Running integration tests using $(PYTEST_BIN) on: $$FILES"; \
	MCDC_MODULE_PATH="$(TARGET)" "$(PYTEST_BIN)" -q $$FILES

# --- Utility -----------------------------------------------------------------
clean:
	rm -rf build build-debug build-tests

install: all
	mkdir -p $(DESTDIR)$(PREFIX)/lib
	cp $(TARGET) $(DESTDIR)$(PREFIX)/lib/

print-%:
	@echo '$*=$($*)'

.PHONY: all debug clean install
