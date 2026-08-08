#include <complex>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "qdata.h"

struct MyIntVector {
    std::vector<int> values;
};

struct AttributedIntVector {
    std::vector<int> values;
    std::vector<std::string> attr_names;
    std::vector<qdata::writable> attr_values;

    auto begin() const { return values.begin(); }
    auto end() const { return values.end(); }
    std::size_t size() const { return values.size(); }
};

namespace qdata {

template <>
struct INTSXP_traits<MyIntVector> {
    static constexpr bool direct = false;

    static std::size_t size(const MyIntVector& x) { return x.values.size(); }
    static std::int32_t get(const MyIntVector& x, std::size_t i) {
        return static_cast<std::int32_t>(x.values[i]);
    }
};

template <>
struct INTSXP_traits<AttributedIntVector> {
    static constexpr bool direct = false;

    static std::size_t size(const AttributedIntVector& x) { return x.values.size(); }
    static std::int32_t get(const AttributedIntVector& x, std::size_t i) {
        return static_cast<std::int32_t>(x.values[i]);
    }
};

template <>
struct ATTRSXP_traits<AttributedIntVector> {
    static constexpr bool has_attributes = true;

    static std::size_t size(const AttributedIntVector& x) {
        return x.attr_names.size();
    }

    static std::string_view name(const AttributedIntVector& x, std::size_t i) {
        return x.attr_names[i];
    }

    static const writable& get(const AttributedIntVector& x, std::size_t i) {
        return x.attr_values[i];
    }
};

} // namespace qdata

namespace {

std::string out_path(const std::filesystem::path& dir, const std::string& name) {
    return (dir / name).string();
}

template <class T>
void save_case(const std::filesystem::path& dir, const std::string& name, const T& x) {
    qdata::save(out_path(dir, name + ".qdata"), x);
}

} // namespace

int main(int argc, char** argv) {
    if(argc != 2) {
        return 1;
    }

    const std::filesystem::path outdir(argv[1]);
    std::filesystem::create_directories(outdir);

    save_case(outdir, "logical_core",
              qdata::logical_vector{{qdata::true_logical, qdata::false_logical, qdata::na_logical}, {}});
    save_case(outdir, "logical_bool", std::vector<bool>{true, false, true});
    save_case(outdir, "logical_optional", std::vector<std::optional<bool>>{true, std::nullopt, false});

    save_case(outdir, "integer_core",
              qdata::integer_vector{{1, qdata::na_int32, 3}, {}});
    save_case(outdir, "integer_int32", std::vector<std::int32_t>{1, 2, 3});
    save_case(outdir, "integer_int", std::vector<int>{4, 5, 6});
    save_case(outdir, "integer_optional", std::vector<std::optional<std::int32_t>>{7, std::nullopt, 9});

    save_case(outdir, "real_core", qdata::real_vector{{1.5, 2.5}, {}});
    save_case(outdir, "real_double", std::vector<double>{3.25, 4.5});
    save_case(outdir, "real_float", std::vector<float>{5.25f, 6.5f});

    save_case(outdir, "complex_core",
              qdata::complex_vector{{std::complex<double>(1.0, 2.0), std::complex<double>(3.0, 4.0)}, {}});
    save_case(outdir, "complex_std",
              std::vector<std::complex<double>>{std::complex<double>(5.0, 6.0), std::complex<double>(7.0, 8.0)});

    save_case(outdir, "string_core",
              qdata::string_vector(std::vector<std::optional<std::string>>{
                  std::optional<std::string>("alpha"),
                  std::nullopt,
                  std::optional<std::string>("")
              }, {}));
    save_case(outdir, "string_std", std::vector<std::string>{"beta", "gamma", ""});
    save_case(outdir, "string_optional",
              std::vector<std::optional<std::string>>{std::optional<std::string>("delta"), std::nullopt, std::optional<std::string>("epsilon")});
    save_case(outdir, "string_view",
              std::vector<std::string_view>{std::string_view("theta"), std::string_view("lambda")});

    save_case(outdir, "raw_core",
              qdata::raw_vector{{std::byte{0x01}, std::byte{0x7F}, std::byte{0xFF}}, {}});
    save_case(outdir, "raw_byte",
              std::vector<std::byte>{std::byte{0x10}, std::byte{0x20}, std::byte{0x30}});
    save_case(outdir, "raw_uint8",
              std::vector<std::uint8_t>{0x40, 0x50, 0x60});

    qdata::list_vector core_list;
    core_list.values.emplace_back(qdata::object(qdata::integer_vector{{10, 20}, {}}));
    core_list.values.emplace_back(qdata::object(
        qdata::string_vector(std::vector<std::optional<std::string>>{
            std::optional<std::string>("zeta")
        }, {})
    ));
    save_case(outdir, "list_core", core_list);

    save_case(outdir, "list_iterable",
              std::vector<MyIntVector>{MyIntVector{{1, 2, 3}}, MyIntVector{{4, 5}}});

    std::vector<std::string> ref_strings{"one", "two"};
    std::vector<int> ref_ints{11, 12, 13};
    save_case(outdir, "list_writable",
              std::vector<qdata::writable>{
                  qdata::writable::ref(ref_strings),
                  qdata::writable::ref(ref_ints),
                  qdata::writable::own(std::vector<std::string>{"owned"})
              });

    std::vector<std::string> attr_label{"numbers"};
    std::vector<MyIntVector> attr_groups{MyIntVector{{1, 2}}, MyIntVector{{3, 4}}};
    AttributedIntVector attributed{
        {21, 22, 23},
        {"label", "groups"},
        {qdata::writable::ref(attr_label), qdata::writable::ref(attr_groups)}
    };
    save_case(outdir, "integer_with_attrs", attributed);

    return 0;
}
