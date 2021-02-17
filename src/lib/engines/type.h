#pragma once

namespace cyber
{
    using id_t = uint32_t;
    using len_t = uint32_t;
    using checksum_t = uint64_t;
    using num_t = uint32_t;
    using offset_t = uint32_t;

    enum MemorySize : uint64_t
    {
        kb = 1 << 10,
        mb = 1 << 20,
        gb = 1 << 30,
    };
}