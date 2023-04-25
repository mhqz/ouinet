#pragma once

#include <boost/asio/windows/stream_handle.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/filesystem.hpp>

#include "signal.h"
#include "../namespaces.h"
#include "../or_throw.h"

namespace ouinet { namespace util { namespace file_io {

asio::windows::stream_handle
open_or_create(const asio::executor&, const fs::path&, sys::error_code&);

asio::windows::stream_handle
open_readonly(const asio::executor&, const fs::path&, sys::error_code&);

// Duplicate the descriptor, see dup(2).
// The descriptor shares offset and flags with that of the original file,
// but it stays open regardless of the original one getting closed,
// so it must be closed separately.
int dup_fd(asio::windows::stream_handle&, sys::error_code&);

void fseek(asio::windows::stream_handle&, size_t pos, sys::error_code&);

size_t current_position(asio::windows::stream_handle&, sys::error_code&);

size_t file_size(asio::windows::stream_handle&, sys::error_code&);

size_t file_remaining_size(asio::windows::stream_handle&, sys::error_code&);

void truncate( asio::windows::stream_handle&
             , size_t new_length
             , sys::error_code&);

void read( asio::windows::stream_handle&
         , asio::mutable_buffer
         , Cancel&
         , asio::yield_context);

void write( asio::windows::stream_handle&
          , asio::const_buffer
          , Cancel&
          , asio::yield_context);

// Check whether the directory exists, if not, try to create it.
// If the directory doesn't exist nor it can be created, the error
// code is set. Returns true if the directory has been created.
bool check_or_create_directory(const fs::path&, sys::error_code&);

template<typename T>
T read_number( asio::windows::stream_handle& f
             , Cancel& cancel
             , asio::yield_context yield)
{
    T num;
    sys::error_code ec;
    // TODO: endianness? (also for writing)
    read(f, asio::buffer(&num, sizeof(num)), cancel, yield[ec]);
    return or_throw<T>(yield, ec, std::move(num));
}

template<typename T>
void write_number( asio::windows::stream_handle& f
                 , T num
                 , Cancel& cancel
                 , asio::yield_context yield)
{
    sys::error_code ec;
    // TODO: endianness? (also for reading)
    write(f, asio::buffer(&num, sizeof(num)), cancel, yield[ec]);
    return or_throw(yield, ec);
}

void remove_file(const fs::path& p);

}}} // namespaces
