#include <complex>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "qdata.h"

namespace {

static_assert(!std::is_copy_constructible<qdata::detail::string_storage_builder>::value);
static_assert(!std::is_move_constructible<qdata::detail::string_storage_builder>::value);

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

template <class Vector, class Values>
void expect_vector_payload(const qdata::object& obj, const Values& expected) {
    const auto* payload = qdata::get_if<Vector>(&obj);
    if(payload == nullptr) {
        throw std::runtime_error("unexpected root payload type");
    }
    if(payload->values != expected) {
        throw std::runtime_error("root payload mismatch");
    }
}

template <class T>
qdata::object roundtrip_payload(const T& input) {
    const auto serialized = qdata::serialize(input, 3, true, 2);
    return qdata::deserialize(serialized, false, 2);
}

void expect_string_payload(
    const qdata::object& obj,
    const std::vector<std::optional<std::string>>& expected
) {
    const auto* payload = qdata::get_if<qdata::string_vector>(&obj);
    if(payload == nullptr || payload->size() != expected.size()) {
        throw std::runtime_error("string root payload mismatch");
    }

    for(std::size_t i = 0; i < expected.size(); ++i) {
        if(payload->is_na(i) != !expected[i].has_value()) {
            throw std::runtime_error("string root NA mismatch");
        }
        if(expected[i] && (*payload)[i].view() != std::string_view(*expected[i])) {
            throw std::runtime_error("string root value mismatch");
        }
    }
}

void expect_root_payload_roundtrips() {
    expect_vector_payload<qdata::logical_vector>(
        roundtrip_payload(std::vector<bool>{true, false, true}),
        std::vector<std::int32_t>{qdata::true_logical, qdata::false_logical, qdata::true_logical}
    );
    expect_vector_payload<qdata::integer_vector>(
        roundtrip_payload(std::vector<std::int32_t>{1, 2, 3}),
        std::vector<std::int32_t>{1, 2, 3}
    );
    expect_vector_payload<qdata::real_vector>(
        roundtrip_payload(std::vector<double>{1.25, 2.5, 5.0}),
        std::vector<double>{1.25, 2.5, 5.0}
    );
    expect_vector_payload<qdata::complex_vector>(
        roundtrip_payload(std::vector<std::complex<double>>{{1.0, 2.0}, {3.0, 4.0}}),
        std::vector<std::complex<double>>{{1.0, 2.0}, {3.0, 4.0}}
    );
    expect_string_payload(
        roundtrip_payload(
            std::vector<std::optional<std::string>>{std::string("alpha"), std::nullopt, std::string("")}
        ),
        std::vector<std::optional<std::string>>{std::string("alpha"), std::nullopt, std::string("")}
    );
    expect_vector_payload<qdata::raw_vector>(
        roundtrip_payload(std::vector<std::byte>{std::byte{0x01}, std::byte{0x7f}, std::byte{0xff}}),
        std::vector<std::byte>{std::byte{0x01}, std::byte{0x7f}, std::byte{0xff}}
    );
}

void expect_adaptive_independent_string_storage() {
    qdata::string_vector scalar(
        std::vector<std::optional<std::string>>{std::string("x")}
    );
    if(!scalar.storage || scalar.storage->capacity_bytes > 256) {
        throw std::runtime_error("scalar string vector reserved too much storage");
    }

    qdata::list_vector input;
    for(std::size_t i = 0; i < 100; ++i) {
        input.values.emplace_back(qdata::object(qdata::string_vector(
            std::vector<std::optional<std::string>>{std::string("x")}
        )));
    }

    auto output = roundtrip_payload(input);
    auto* list = qdata::get_if<qdata::list_vector>(&output);
    if(list == nullptr || list->size() != 100) {
        throw std::runtime_error("scalar string list roundtrip mismatch");
    }

    std::unordered_set<const qdata::string_storage*> storage_owners;
    std::size_t total_capacity = 0;
    for(std::size_t i = 0; i < list->size(); ++i) {
        const auto* strings = qdata::get_if<qdata::string_vector>(&(*list)[i]);
        if(strings == nullptr || strings->size() != 1 || (*strings)[0].view() != "x") {
            throw std::runtime_error("scalar string child roundtrip mismatch");
        }
        if(!strings->storage || !storage_owners.insert(strings->storage.get()).second) {
            throw std::runtime_error("deserialized string vectors share storage");
        }
        total_capacity += strings->storage->capacity_bytes;
    }

    if(total_capacity > list->size() * 256) {
        throw std::runtime_error("scalar string vectors reserved too much storage");
    }

    qdata::string_vector kept = *qdata::get_if<qdata::string_vector>(&(*list)[0]);
    std::weak_ptr<const qdata::string_storage> discarded =
        qdata::get_if<qdata::string_vector>(&(*list)[1])->storage;
    output = qdata::object{};
    if(!discarded.expired()) {
        throw std::runtime_error("discarded sibling string storage remains alive");
    }
    if(!kept.storage || kept[0].view() != "x") {
        throw std::runtime_error("extracted string vector lost its storage");
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

    debug_log("roundtrip every nonempty root payload type");
    expect_root_payload_roundtrips();

    debug_log("adaptive independent string storage");
    expect_adaptive_independent_string_storage();

    debug_log("done");
    return 0;
}
