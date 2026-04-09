library(qs2)

cat("Testing qdata_cpp_external.h downstream C++ wrappers...\n")

stopifnot(requireNamespace("Rcpp", quietly = TRUE))

include_dir <- system.file("include", package = "qs2")
stopifnot(nzchar(include_dir))
stopifnot(file.exists(file.path(include_dir, "qdata_cpp_external.h")))

old_r_tests <- Sys.getenv("R_TESTS", unset = NA_character_)
on.exit({
  if (is.na(old_r_tests)) {
    Sys.unsetenv("R_TESTS")
  } else {
    Sys.setenv(R_TESTS = old_r_tests)
  }
}, add = TRUE)

if (!is.na(old_r_tests) && nzchar(old_r_tests)) {
  normalized_r_tests <- tryCatch(
    normalizePath(old_r_tests, winslash = "/", mustWork = TRUE),
    error = function(...) old_r_tests
  )
  Sys.setenv(R_TESTS = normalized_r_tests)
}

source_file <- tempfile("qdata-cpp-external-", fileext = ".cpp")
on.exit(unlink(source_file), add = TRUE)

probe_source <- paste(
  "// [[Rcpp::depends(qs2)]]",
  "// [[Rcpp::plugins(cpp17)]]",
  "#include <Rcpp.h>",
  "#include <cstddef>",
  "#include <cstdint>",
  "#include <string>",
  "#include <vector>",
  "",
  "#include \"qdata_cpp_external.h\"",
  "",
  "namespace {",
  "",
  "qdata_ext::object make_nested_object(const std::size_t depth) {",
  "    qdata_ext::object current(qdata_ext::nil_value{});",
  "    for(std::size_t i = 0; i < depth; ++i) {",
  "        qdata_ext::list_vector next;",
  "        next.values.emplace_back(std::move(current));",
  "        current = qdata_ext::object(std::move(next));",
  "    }",
  "    return current;",
  "}",
  "",
  "Rcpp::IntegerVector as_integer_vector(const qdata_ext::object& obj) {",
  "    if(!qdata_ext::holds_alternative<qdata_ext::integer_vector>(obj)) {",
  "        Rcpp::stop(\"unexpected qdata payload type\");",
  "    }",
  "    const auto& values = qdata_ext::get<qdata_ext::integer_vector>(obj).values;",
  "    return Rcpp::IntegerVector(values.begin(), values.end());",
  "}",
  "",
  "} // namespace",
  "",
  "// [[Rcpp::export]]",
  "Rcpp::List qdata_cpp_external_probe(const std::string& file_path) {",
  "    const std::vector<std::int32_t> input{1, 2, 3, 4};",
  "    const auto bytes = qdata_ext::serialize(input);",
  "    const auto chars = qdata_ext::serialize<std::vector<char>>(input);",
  "    const auto text = qdata_ext::serialize<std::string>(input);",
  "    qdata_ext::save(file_path, input);",
  "    return Rcpp::List::create(",
  "        Rcpp::Named(\"bytes\") = as_integer_vector(qdata_ext::deserialize(bytes)),",
  "        Rcpp::Named(\"chars\") = as_integer_vector(qdata_ext::deserialize(chars)),",
  "        Rcpp::Named(\"text\") = as_integer_vector(qdata_ext::deserialize(text)),",
  "        Rcpp::Named(\"ptr\") = as_integer_vector(qdata_ext::deserialize(chars.data(), chars.size())),",
  "        Rcpp::Named(\"file\") = as_integer_vector(qdata_ext::read(file_path))",
  "    );",
  "}",
  "",
  "// [[Rcpp::export]]",
  "bool qdata_cpp_external_default_depth_rejects() {",
  "    try {",
  "        (void) qdata_ext::serialize(make_nested_object(600));",
  "    } catch(const std::runtime_error& err) {",
  "        return std::string(err.what()).find(\"nesting depth exceeds\") != std::string::npos;",
  "    }",
  "    return false;",
  "}",
  "",
  "// [[Rcpp::export]]",
  "bool qdata_cpp_external_override_depth_succeeds() {",
  "    const auto bytes = qdata_ext::serialize(make_nested_object(600), 3, true, 1, 1024);",
  "    return qdata_ext::holds_alternative<qdata_ext::list_vector>(",
  "        qdata_ext::deserialize(bytes, false, 1, 1024)",
  "    );",
  "}",
  sep = "\n"
)

writeLines(probe_source, source_file, useBytes = TRUE)
Rcpp::sourceCpp(file = source_file, rebuild = TRUE, showOutput = TRUE)

tmp_qdata <- tempfile(fileext = ".qdata")
probe <- qdata_cpp_external_probe(tmp_qdata)
expected <- 1:4
stopifnot(identical(probe$bytes, expected))
stopifnot(identical(probe$chars, expected))
stopifnot(identical(probe$text, expected))
stopifnot(identical(probe$ptr, expected))
stopifnot(identical(probe$file, expected))
stopifnot(isTRUE(qdata_cpp_external_default_depth_rejects()))
stopifnot(isTRUE(qdata_cpp_external_override_depth_succeeds()))

cat("qdata_cpp_external.h tests completed.\n")
