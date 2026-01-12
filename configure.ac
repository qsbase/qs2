AC_INIT([qs2],[0.1.0],[traversc@gmail.com])
AC_PATH_PROG([PKGCONF],[pkg-config],[],[$PATH:/usr/local/bin:ext/bin:ext:/sw/bin:/opt/bin:/opt/local/bin])

########################################################
AC_LANG(C++)
: ${R_HOME=`R RHOME`}
PATH_TO_CPP_COMPILER=`"${R_HOME}/bin/R" CMD config CXX`
AC_PROG_CXX([$PATH_TO_CPP_COMPILER])
echo "C++ compiler: $PATH_TO_CPP_COMPILER"

########################################################
### Configure args

AC_ARG_WITH([zstd-force-compile],
            AS_HELP_STRING([--with-zstd-force-compile],[Force compilation of bundled zstd source files]),
            [zstd_force_compile="true"])

AC_ARG_WITH([zstd-include],
            AS_HELP_STRING([--with-zstd-include=INCLUDE_PATH],[the location of zstd header files]),
            [zstd_include_path=$withval])

AC_ARG_WITH([zstd-lib],
            AS_HELP_STRING([--with-zstd-lib=LIB_PATH],[the location of zstd library files]),
            [zstd_lib_path=$withval])

# TBB selection:
# - default: auto (let RcppParallel decide)
# - --without-TBB: force disable by setting RCPP_PARALLEL_USE_TBB=0
using_tbb="auto"
AC_ARG_WITH([TBB],
            AS_HELP_STRING([--without-TBB],[Disable TBB usage and force RCPP_PARALLEL_USE_TBB=0]),
            [if test "x$withval" = "xno"; then using_tbb="false"; fi])

AC_ARG_WITH([dynamic-blocksize],
            AS_HELP_STRING([--with-dynamic-blocksize],[Dynamic blocksize, for debugging and testing ONLY]),
            [using_dynamic_blocksize="true"])

AC_ARG_WITH([simd],
            AS_HELP_STRING([--with-simd],[Manually select SIMD support (options: AVX2, SSE2)]),
            [with_simd=$withval])

########################################################
#### Version value function

getVersion()
{
VERSION_STRING=$1
MAJOR=`echo $VERSION_STRING | cut -d. -f1`
MINOR=`echo $VERSION_STRING | cut -d. -f2`
RELEASE=`echo $VERSION_STRING | cut -d. -f3`
echo $(($MAJOR*100000+$MINOR*100+$RELEASE))
}

########################################################
#### Compile ZSTD checks

ZSTD_INCLUDE_PATH=""
ZSTD_LIBS=""
ZSTD_SHLIB=""
ZSTD_CLEAN=""

if test xx$zstd_force_compile = "xxtrue"; then
  echo "Compiling zstd from source due to --with-zstd-force-compile"
  COMPILE_ZSTD="true"
elif test "xx$zstd_include_path" != "xx"; then
  echo "Using user-defined zstd install paths"
    ZSTD_LIBS="-L${zstd_lib_path}"
    ZSTD_INCLUDE_PATH="-I${zstd_include_path}"
    COMPILE_ZSTD="false"
elif test "xx$PKGCONF" != "xx"; then
  if "${PKGCONF}" --exists libzstd; then
    VERSION_STRING=`${PKGCONF} --modversion libzstd`
    VER=`getVersion ${VERSION_STRING}`
    if test "${VER}" -ge 100506; then
      echo "zstd ${VERSION_STRING} library detected -- skipping zstd compilation"
      ZSTD_LIBS=`"${PKGCONF}" --libs libzstd`
      ZSTD_INCLUDE_PATH=`"${PKGCONF}" --cflags-only-I libzstd`
      COMPILE_ZSTD="false"
    else
      echo "zstd ${VERSION_STRING} library detected but is lower than bundled version (1.5.2) -- compiling from source"
      COMPILE_ZSTD="true"
    fi
  else
    echo "zstd library not detected -- compiling from source"
    COMPILE_ZSTD="true"
  fi
else
  echo "pkg-config not detected -- compiling zstd from source"
  COMPILE_ZSTD="true"
fi

if test xx$COMPILE_ZSTD = "xxtrue"; then
  ZSTD_LIBS="${LIBS} -lQSZSTD"
  ZSTD_INCLUDE_PATH="-IZSTD -IZSTD/common -IZSTD/decompress -IZSTD/compress"
  ZSTD_SHLIB="libQSZSTD.a"
  ZSTD_CLEAN="\$(LIBZSTD) libQSZSTD.a"
fi

########################################################
#### Dynamic blocksize

if test xx$using_dynamic_blocksize = "xxtrue"; then
  echo "Using dynamic blocksize (for debugging and testing ONLY)"
  DYNAMIC_BLOCKSIZE_FLAG="-DQS2_DYNAMIC_BLOCKSIZE=1"
else
  echo "Fixed blocksize"
fi

########################################################
#### Check if -latomic flag is needed (relevant when TBB may be used)

m4_define([QS2_ATOMIC_TEST_PROGRAM], [[
  #include <atomic>
  #include <cstdint>
  std::atomic<std::uint64_t> x1{0};
  std::atomic<std::uint8_t> x2{0};
  std::atomic<char*> x3{nullptr};
  std::atomic<bool> x4{false};
  int main() {
    x1.fetch_add(1);
    x2.fetch_add(1);
    char temp = 'a';
    x3.store(&temp);
    x4.store(true);
    return 0;
  }
]])

if test xx$using_tbb != "xxfalse"; then
  echo "Checking if atomic link works without -latomic"
  AC_LINK_IFELSE([AC_LANG_SOURCE([QS2_ATOMIC_TEST_PROGRAM])],
                 [LINK_WORKS_WITHOUT_LIBATOMIC="yes"],
                 [LINK_WORKS_WITHOUT_LIBATOMIC="no"])

  if test xx$LINK_WORKS_WITHOUT_LIBATOMIC = "xxyes"; then
    COMPILER_SPECIFIC_LIBS=""
  else
    echo "Checking if atomic link works with -latomic"
    save_LIBS="$LIBS"
    LIBS="$LIBS -latomic"
    AC_LINK_IFELSE([AC_LANG_SOURCE([QS2_ATOMIC_TEST_PROGRAM])],
                   [LINK_WORKS_WITH_LIBATOMIC="yes"],
                   [LINK_WORKS_WITH_LIBATOMIC="no"])
    LIBS="$save_LIBS"

    if test xx$LINK_WORKS_WITH_LIBATOMIC = "xxyes"; then
      COMPILER_SPECIFIC_LIBS="-latomic"
    else
      echo "Atomic link failed even with -latomic, disabling TBB"
      using_tbb="false"
      COMPILER_SPECIFIC_LIBS=""
    fi
  fi
else
  COMPILER_SPECIFIC_LIBS=""
fi

########################################################
#### TBB selection (default: auto)

if test xx$using_tbb = "xxfalse"; then
  echo "TBB disabled by configure flag or missing libatomic"
  TBB_FLAG="-DRCPP_PARALLEL_USE_TBB=0"
else
  echo "TBB use determined by RcppParallel"
  TBB_FLAG=""
fi

########################################################
#### Compile SIMD checks

SIMD_FLAG=""

if test xx$with_simd = "xxAVX2"; then
  echo "Using AVX2"
  SIMD_FLAG="-mavx2"
elif test xx$with_simd = "xxSSE2"; then
  echo "Using SSE2"
  SIMD_FLAG="-msse2"
fi

########################################################

AC_SUBST([COMPILER_SPECIFIC_LIBS], $COMPILER_SPECIFIC_LIBS)
AC_SUBST([ZSTD_INCLUDE_PATH], $ZSTD_INCLUDE_PATH)
AC_SUBST([ZSTD_LIBS], $ZSTD_LIBS)
AC_SUBST([ZSTD_SHLIB], $ZSTD_SHLIB)
AC_SUBST([ZSTD_CLEAN], $ZSTD_CLEAN)
AC_SUBST([DYNAMIC_BLOCKSIZE_FLAG], $DYNAMIC_BLOCKSIZE_FLAG)
AC_SUBST([TBB_FLAG], $TBB_FLAG)
AC_SUBST([SIMD_FLAG], $SIMD_FLAG)

AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
