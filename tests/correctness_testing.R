total_time <- Sys.time()

if(require("qs2", quietly=TRUE) &&
   require("dplyr", quietly=TRUE) &&
   require("data.table", quietly=TRUE) &&
   require("stringfish", quietly=TRUE) &&
   require("stringi", quietly=TRUE)
) {

options(warn = 1)

do_gc <- function() {
  if (utils::compareVersion(as.character(getRversion()), "3.5.0") != -1) {
    gc(full = TRUE)
  } else {
    gc()
  }
}

args <- commandArgs(TRUE)
if (nchar(Sys.getenv("QS_EXTENDED_TESTS")) == 0) {
  do_extended_tests <- FALSE
  mode <- "filestream"
  reps <- 1
  internal_reps <- 1
  test_points <- c(1e5)
  test_points_raw_vector <- c(1e5)
  test_points_character_vector <- c(1e3)
  max_size <- 1e5
  random_threads <- 1
  random_cl <- 1
} else {
  cat("performing extended tests\n")
  do_extended_tests <- TRUE
  reps <- 3
  internal_reps <- 2
  if (length(args) == 0) {
    mode <- "filestream"
  } else {
    mode <- args[1] # fd, memory, HANDLE, filestream
  }
  test_points <- c(0, 1, 2, 4, 8, 2^5 - 1, 2^5 + 1, 2^5, 2^8 - 1, 2^8 + 1, 2^8, 2^16 - 1, 2^16 + 1, 2^16, 1e6, 1e7)
  # Raw vector correctness, test block boundaries too
  test_points_raw_vector <- c(test_points, seq(2^19-1000,2^19)) %>% sort
  test_points_character_vector <- test_points
  max_size <- 1e7
  random_cl <- 10
  if(qs2:::check_TBB() == FALSE) {
    cat("TBB not detected\n")
    random_threads <- 1 # no TBB support
  } else {
    cat("TBB detected\n")
    random_threads <- 5
  }
}
myfile <- tempfile()

obj_size <- 0
get_obj_size <- function() {
  get("obj_size", envir = globalenv())
}
set_obj_size <- function(x) {
  assign("obj_size", get_obj_size() + as.numeric(object.size(x)), envir = globalenv())
  return(get_obj_size());
}

rand_strings <- function(N) {
  if(N == 0) {
    character(0)
  } else {
    stringi::stri_rand_strings(N, round(rexp(N, 1/90)))
  }
}

# do not include functions as they do not evaluate to TRUE with identical(x, y)
random_object_generator <- function(N, with_envs = FALSE) { # additional input: global obj_size, max_size
  if (sample(3, 1) == 1) {
    ret <- as.list(1:N)
  } else if (sample(2, 1) == 1) {
    ret <- as.pairlist(1:N)
  } else {
    ret <- as.pairlist(1:N)
  }
  for (i in 1:N) {
    if (get_obj_size() > get("max_size", envir = globalenv())) break;
    otype <- sample(12, size = 1)
    z <- NULL
    is_attribute <- ifelse(i == 1, F, sample(c(F, T), size = 1))
    if (otype == 1) {z <- rnorm(1e4); set_obj_size(z);}
    else if (otype == 2) { z <- sample(1e4) - 5e2; set_obj_size(z); }
    else if (otype == 3) { z <- sample(c(T, F, NA), size = 1e4, replace = TRUE); set_obj_size(z); }
    else if (otype == 4) { z <- (sample(256, size = 1e4, replace = TRUE) - 1) %>% as.raw; set_obj_size(z); }
    else if (otype == 5) { z <- replicate(sample(1e4, size = 1), {rep(letters, length.out = sample(10, size = 1)) %>% paste(collapse = "")}); set_obj_size(z); }
    else if (otype == 6) { z <- rep(letters, length.out = sample(1e4, size = 1)) %>% paste(collapse = ""); set_obj_size(z); }
    else if (otype == 7) { z <- as.formula("y ~ a + b + c : d", env = globalenv()); attr(z, "blah") <- sample(1e4) - 5e2; set_obj_size(z); }
    else if (with_envs && otype %in% c(8, 9)) { z <- function(x) {x + runif(1)} }
    else { z <- random_object_generator(N, with_envs) }
    if (is_attribute) {
      attr(ret[[i - 1]], runif(1) %>% as.character()) <- z
    } else {
      ret[[i]] <- z
    }
  }
  return(ret)
}

nested_tibble <- function() {
  sub_tibble <- function(nr = 600, nc = 4) {
    z <- lapply(1:nc, function(i) rand_strings(nr)) %>%
      setNames(make.unique(paste0(sample(letters, nc), rand_strings(nc)))) %>%
      bind_cols %>%
      as_tibble
  }
  tibble(
    col1 = rand_strings(100),
    col2 = rand_strings(100),
    col3 = lapply(1:100, function(i) sub_tibble(nr = 600, nc = 4)),
    col4 = lapply(1:100, function(i) sub_tibble(nr = 600, nc = 4)),
    col5 = lapply(1:100, function(i) sub_tibble(nr = 600, nc = 4))
  ) %>% setNames(make.unique(paste0(sample(letters, 5), rand_strings(5))))
}

printCarriage <- function(x) {
  cat(x, "\r")
}

################################################################################################

qs_save_rand <- function(x) {
  nt <- sample.int(random_threads, 1)
  cl <- sample.int(random_cl,1)
  if(format == "qs2") {
    qs2::qs_save(x, file = myfile, compress_level = cl, nthreads = nt)
  } else if(format == "qdata") {
    qs2::qd_save(x, file = myfile, compress_level = cl, nthreads = nt)
  }
}

qs_read_rand <- function() {
  ar <- sample(c(TRUE, FALSE),1)
  nt <- sample(random_threads, 1)
  if(format == "qs2") {
    qs2::qs_read(myfile, validate_checksum=check, nthreads = nt)
  } else if(format == "qdata") {
    qs2::qd_read(myfile, use_alt_rep = ar, validate_checksum=check, nthreads = nt)
  }
}

################################################################################################
for(format in c("qdata", "qs2")) {
for(check in c(TRUE, FALSE)) {
for (q in 1:reps) {
  cat("########################################")
  cat("Format", format, "using checksum", check, "rep",  q, "of", reps, "\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points) {
    for (i in 1:internal_reps) {
      x1 <- rep(letters, length.out = tp) %>% paste(collapse = "")
      x1 <- c(NA, "", x1)
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("strings: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points_raw_vector) {
    for (i in 1:internal_reps) {
      x1 <- list(raw(tp), a="a")
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      # dont do gc in this loop, too slow
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Raw vector: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")
  

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points_character_vector) {
    for (i in 1:internal_reps) {
      x1 <- c(NA, "", rand_strings(tp))
      qs_save_rand(x1)
      time[i] <- Sys.time()
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Character Vectors: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points_character_vector) {
    for (i in 1:internal_reps) {
      x1 <- c(NA, "", rand_strings(tp))
      x1 <- stringfish::convert_to_sf(x1)
      qs_save_rand(x1)
      time[i] <- Sys.time()
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Stringfish: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points) {
    for (i in 1:internal_reps) {
      x1 <- sample(1:tp, replace = TRUE)
      x1 <- c(NA, x1)
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Integers: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points) {
    for (i in 1:internal_reps) {
      x1 <- rnorm(tp)
      x1 <- c(NA, x1)
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Numeric: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points) {
    for (i in 1:internal_reps) {

      x1 <- sample(c(T, F, NA), replace = TRUE, size = tp)
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Logical: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points) {
    for (i in 1:internal_reps) {
      x1 <- as.list(runif(tp))
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("List: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")

  time <- vector("numeric", length = internal_reps)
  for (tp in test_points) {
    for (i in 1:internal_reps) {
      re <- rnorm(tp)
      im <- runif(tp)
      x1 <- complex(real = re, imaginary = im)
      x1 <- c(NA_complex_, x1)
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Complex: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")

  for (i in 1:internal_reps) {
    x1 <- mtcars
    qs_save_rand(x1)
    z <- qs_read_rand()
    do_gc()
    stopifnot(identical(z, x1))
  }
  cat("Data.frame test")
  cat("\n")

  # reserve below section for extended tests
  if(!do_extended_tests) {
    next;
  }

  if(format == "qs2") {
    for (i in 1:internal_reps) {
      x1 <- rep( replicate(1000, { rep(letters, length.out = 2^7 + sample(10, size = 1)) %>% paste(collapse = "") }), length.out = 1e6 )
      x1 <- data.table(str = x1,num = runif(1:1e6))
      qs_save_rand(x1)
      z <- qs_read_rand()
      do_gc()
      stopifnot(all(z == x1))
    }
    cat("Data.table test")
    cat("\n")
  }

  for (i in 1:internal_reps) {
    x1 <- rep( replicate(1000, { rep(letters, length.out = 2^7 + sample(10, size = 1)) %>% paste(collapse = "") }), length.out = 1e6 )
    x1 <- tibble(str = x1,num = runif(1:1e6))
    qs_save_rand(x1)
    z <- qs_read_rand()
    do_gc()
    stopifnot(identical(z, x1))
  }
  cat("Tibble test")
  cat("\n")

  if (Sys.info()[['sysname']] != "Windows") {
    for (i in 1:internal_reps) {
      x1 <- "己所不欲，勿施于人" # utf 8
      x2 <- x1
      Encoding(x2) <- "latin1"
      x3 <- x1
      Encoding(x3) <- "bytes"
      x4 <- rep(x1, x2, length.out = 1e4) %>% paste(collapse = ";")
      x1 <- c(x1, x2, x3, x4)
      qs_save_rand(x1)
      z <- qs_read_rand()
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage("Encoding test")
  } else {
    printCarriage("(Encoding test not run on windows)")
  }
  cat("\n")

  for (tp in test_points) {
    time <- vector("numeric", length = internal_reps)
    for (i in 1:internal_reps) {
      x1 <- factor(rep(letters, length.out = tp), levels = sample(letters), ordered = TRUE)
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Factors: %s, %s s",tp, signif(mean(time), 4)))
  }
  cat("\n")

  if(format == "qs2") {
    time <- vector("numeric", length = 8)
    for (i in 1:8) {
      obj_size <- 0
      x1 <- random_object_generator(12)
      printCarriage(sprintf("Random objects: %s bytes", object.size(x1) %>% as.numeric))
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      time[i] <- Sys.time() - time[i]
      do_gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Random objects: %s s", signif(mean(time), 4)))
    cat("\n")
  }

  time <- vector("numeric", length = internal_reps)
  for (i in 1:internal_reps) {
    x1 <- as.list(1:26)
    attr(x1[[26]], letters[26]) <- rnorm(100)
    for (i in 25:1) {
      attr(x1[[i]], letters[i]) <- x1[[i + 1]]
    }
    time[i] <- Sys.time()
    qs_save_rand(x1)
    z <- qs_read_rand()
    time[i] <- Sys.time() - time[i]
    do_gc()
    stopifnot(identical(z, x1))
  }
  printCarriage(sprintf("Nested attributes: %s s", signif(mean(time), 4)))
  cat("\n")

  # alt-rep -- should serialize the unpacked object
  time <- vector("numeric", length = internal_reps)
  for (i in 1:internal_reps) {
    x1 <- 1:max_size
    time[i] <- Sys.time()
    qs_save_rand(x1)
    z <- qs_read_rand()
    time[i] <- Sys.time() - time[i]
    do_gc()
    stopifnot(identical(z, x1))
  }
  printCarriage(sprintf("Alt rep integer: %s s", signif(mean(time), 4)))
  cat("\n")


  if(format == "qs2") {
    time <- vector("numeric", length = internal_reps)
    for (i in 1:internal_reps) {
      x1 <- new.env()
      x1[["a"]] <- 1:max_size
      x1[["b"]] <- runif(max_size)
      x1[["c"]] <- stringfish::random_strings(1e4, vector_mode = "normal")
      time[i] <- Sys.time()
      qs_save_rand(x1)
      z <- qs_read_rand()
      stopifnot(identical(z[["a"]], x1[["a"]]))
      stopifnot(identical(z[["b"]], x1[["b"]]))
      stopifnot(identical(z[["c"]], x1[["c"]]))
      time[i] <- Sys.time() - time[i]
      do_gc()
    }
    printCarriage(sprintf("Environment test: %s s", signif(mean(time), 4)))
    cat("\n")
  }

  time <- vector("numeric", length = internal_reps)
  for (i in 1:internal_reps) {
    x1 <- nested_tibble()
    time[i] <- Sys.time()
    qs_save_rand(x1)
    z <- qs_read_rand()
    stopifnot(identical(z, x1))
    time[i] <- Sys.time() - time[i]
    do_gc()
  }
  printCarriage(sprintf("nested tibble test: %s s", signif(mean(time), 4)))
  cat("\n")
} # end format
} # end check
} # end reps

} # end requireNamespace