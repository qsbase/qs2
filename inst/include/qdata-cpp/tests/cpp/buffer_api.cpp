#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "qdata.h"

namespace {

void debug_log(const char* message) {
    std::cerr << "[qdata_buffer_api] " << message << '\n';
    std::cerr.flush();
}

void expect_integer_payload(const qdata::object& obj, const std::vector<std::int32_t>& expected) {
    if(!qdata::holds_alternative<qdata::integer_vector>(obj)) {
        throw std::runtime_error("unexpected payload type");
    }

    const auto* ints_ptr = qdata::get_if<qdata::integer_vector>(&obj);
    if(ints_ptr == nullptr) {
        throw std::runtime_error("missing integer payload");
    }

    const auto& ints = qdata::get<qdata::integer_vector>(obj);
    if(ints.values != expected) {
        throw std::runtime_error("integer payload mismatch");
    }
}

template <class Buffer>
Buffer serialize_via_erased_api(const std::vector<std::int32_t>& input) {
    Buffer output;
    qdata::detail::serialize_erased_impl(
        static_cast<void*>(std::addressof(output)),
        qdata::detail::make_output_buffer_ops<Buffer>(),
        std::addressof(input),
        &qdata::detail::write_erased<std::vector<std::int32_t>>,
        3,
        true,
        1,
        qdata::detail::default_qdata_max_nesting_depth
    );
    return output;
}

} // namespace

int main() {
    debug_log("start");
    const std::vector<std::int32_t> input{1, 2, 3, 4};

    debug_log("serialize std::vector<std::byte>");
    const auto bytes = qdata::serialize(input);
    debug_log("deserialize std::vector<std::byte>");
    expect_integer_payload(qdata::deserialize(bytes), input);

    debug_log("serialize std::vector<char>");
    const auto chars = qdata::serialize<std::vector<char>>(input);
    debug_log("deserialize std::vector<char>");
    expect_integer_payload(qdata::deserialize(chars), input);

    debug_log("serialize std::vector<std::uint8_t>");
    const auto uints = qdata::serialize<std::vector<std::uint8_t>>(input);
    debug_log("deserialize std::vector<std::uint8_t>");
    expect_integer_payload(qdata::deserialize(uints), input);

    debug_log("serialize std::string");
    const auto text = qdata::serialize<std::string>(input);
    debug_log("deserialize std::string");
    expect_integer_payload(qdata::deserialize(text), input);

    debug_log("serialize_erased_impl std::vector<std::byte>");
    const auto bytes_erased = serialize_via_erased_api<std::vector<std::byte>>(input);
    debug_log("deserialize erased std::vector<std::byte>");
    expect_integer_payload(qdata::deserialize(bytes_erased), input);

    debug_log("serialize_erased_impl std::vector<char>");
    const auto chars_erased = serialize_via_erased_api<std::vector<char>>(input);
    debug_log("deserialize erased std::vector<char>");
    expect_integer_payload(qdata::deserialize(chars_erased), input);

    debug_log("serialize_erased_impl std::vector<std::uint8_t>");
    const auto uints_erased = serialize_via_erased_api<std::vector<std::uint8_t>>(input);
    debug_log("deserialize erased std::vector<std::uint8_t>");
    expect_integer_payload(qdata::deserialize(uints_erased), input);

    debug_log("serialize_erased_impl std::string");
    const auto text_erased = serialize_via_erased_api<std::string>(input);
    debug_log("deserialize erased std::string");
    expect_integer_payload(qdata::deserialize(text_erased), input);

    debug_log("done");
    return 0;
}
