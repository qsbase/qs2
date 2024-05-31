#include "qs_serializer.h"
#include "qs_unserializer.h"
#include "qd_serializer.h"
#include "qd_unserializer.h"

// [[Rcpp::export(rng = false, invisible = true)]]
SEXP qs_save(SEXP object, const std::string & file, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
    UNWIND_PROTECT_BEGIN()
    OfStreamWriter myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_SAVE_ERR_MSG);
    }
    write_qs2_header(myFile, shuffle);
    struct R_outpstream_st out;
    if(nthreads > 1) {
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            BlockCompressWriterMT<OfStreamWriter, ZstdShuffleCompressor> writer(myFile, compress_level, nthreads);
            R_SerializeInit(&out, writer);
            qs_save_impl_args args = {object, &out};
            DO_UNWIND_PROTECT((qs_save_impl<BlockCompressWriterMT<OfStreamWriter, ZstdShuffleCompressor>>), (void*)&args);
        } else {
            BlockCompressWriterMT<OfStreamWriter, ZstdCompressor> writer(myFile, compress_level, nthreads);
            R_SerializeInit(&out, writer);
            qs_save_impl_args args = {object, &out};
            DO_UNWIND_PROTECT((qs_save_impl<BlockCompressWriterMT<OfStreamWriter, ZstdCompressor>>), (void*)&args);
        }
    } else {
        if(shuffle) {
            BlockCompressWriter<OfStreamWriter, ZstdShuffleCompressor> writer(myFile, compress_level);
            R_SerializeInit(&out, writer);
            qs_save_impl_args args = {object, &out};
            DO_UNWIND_PROTECT((qs_save_impl<BlockCompressWriter<OfStreamWriter, ZstdShuffleCompressor>>), (void*)&args);
        } else {
            BlockCompressWriter<OfStreamWriter, ZstdCompressor> writer(myFile, compress_level);
            R_SerializeInit(&out, writer);
            qs_save_impl_args args = {object, &out};
            DO_UNWIND_PROTECT((qs_save_impl<BlockCompressWriter<OfStreamWriter, ZstdCompressor>>), (void*)&args);
        }
    }

    UNWIND_PROTECT_END();
}

// [[Rcpp::export(rng = false)]]
SEXP qs_read(const std::string & file, const int nthreads = 1) {
    UNWIND_PROTECT_BEGIN()
    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_READ_ERR_MSG);
    }
    bool shuffle;
    read_qs2_header(myFile, shuffle);
    struct R_inpstream_st in;
    if(nthreads > 1) {
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            BlockCompressReaderMT<IfStreamReader, ZstdShuffleDecompressor> reader(myFile, nthreads);
            R_UnserializeInit<BlockCompressReaderMT<IfStreamReader, ZstdShuffleDecompressor>>(&in, (R_pstream_data_t)(&reader));
            return DO_UNWIND_PROTECT( (qs_read_impl<BlockCompressReaderMT<IfStreamReader, ZstdShuffleDecompressor>>), &in);
        } else {
            BlockCompressReaderMT<IfStreamReader, ZstdDecompressor> reader(myFile, nthreads);
            R_UnserializeInit<BlockCompressReaderMT<IfStreamReader, ZstdDecompressor>>(&in, (R_pstream_data_t)(&reader));
            return DO_UNWIND_PROTECT( (qs_read_impl<BlockCompressReaderMT<IfStreamReader, ZstdDecompressor>>), &in);
        }
    } else {
        if(shuffle) {
            BlockCompressReader<IfStreamReader, ZstdShuffleDecompressor> reader(myFile);
            R_UnserializeInit<BlockCompressReader<IfStreamReader, ZstdShuffleDecompressor>>(&in, (R_pstream_data_t)(&reader));
            return DO_UNWIND_PROTECT( (qs_read_impl<BlockCompressReader<IfStreamReader, ZstdShuffleDecompressor>>), &in);
        } else {
            BlockCompressReader<IfStreamReader, ZstdDecompressor> reader(myFile);
            R_UnserializeInit<BlockCompressReader<IfStreamReader, ZstdDecompressor>>(&in, (R_pstream_data_t)(&reader));
            return DO_UNWIND_PROTECT( (qs_read_impl<BlockCompressReader<IfStreamReader, ZstdDecompressor>>), &in);
        }
    }

    UNWIND_PROTECT_END();
}


// [[Rcpp::export(rng = false, invisible = true)]]
SEXP qd_save(SEXP object, const std::string & file, const int compress_level = 3, const bool shuffle = true, const bool warn = true, const int nthreads = 1) {
    OfStreamWriter myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_SAVE_ERR_MSG);
    }
    write_qdata_header(myFile, shuffle);
    if(nthreads > 1) {
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            BlockCompressWriterMT<OfStreamWriter, ZstdShuffleCompressor> writer(myFile, compress_level, nthreads);
            QdataSerializer<BlockCompressWriterMT<OfStreamWriter, ZstdShuffleCompressor>> serializer(writer, warn);
            serializer.write_object(object);
            writer.finish();
        } else {
            BlockCompressWriterMT<OfStreamWriter, ZstdCompressor> writer(myFile, compress_level, nthreads);
            QdataSerializer<BlockCompressWriterMT<OfStreamWriter, ZstdCompressor>> serializer(writer, warn);
            serializer.write_object(object);
            writer.finish();
        }
    } else {
        if(shuffle) {
            BlockCompressWriter<OfStreamWriter, ZstdShuffleCompressor> writer(myFile, compress_level);
            QdataSerializer<BlockCompressWriter<OfStreamWriter, ZstdShuffleCompressor>> serializer(writer, warn);
            serializer.write_object(object);
            writer.finish();
        } else {
            BlockCompressWriter<OfStreamWriter, ZstdCompressor> writer(myFile, compress_level);
            QdataSerializer<BlockCompressWriter<OfStreamWriter, ZstdCompressor>> serializer(writer, warn);
            serializer.write_object(object);
            writer.finish();
        }
    }
    return R_NilValue;
}

// [[Rcpp::export(rng = false)]]
SEXP qd_read(const std::string & file, const bool use_alt_rep = false,const int nthreads = 1) {
    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_READ_ERR_MSG);
    }
    bool shuffle;
    read_qdata_header(myFile, shuffle);
    if(nthreads > 1) {
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            BlockCompressReaderMT<IfStreamReader, ZstdShuffleDecompressor> reader(myFile, nthreads);
            QdataUnserializer<BlockCompressReaderMT<IfStreamReader, ZstdShuffleDecompressor>> unserializer(reader, use_alt_rep);
            SEXP output = unserializer.read_object();
            reader.finish();
            return output;
        } else {
            BlockCompressReaderMT<IfStreamReader, ZstdDecompressor> reader(myFile, nthreads);
            QdataUnserializer<BlockCompressReaderMT<IfStreamReader, ZstdDecompressor>> unserializer(reader, use_alt_rep);
            SEXP output = unserializer.read_object();
            reader.finish();
            return output;
        }
    } else {
        if(shuffle) {
            BlockCompressReader<IfStreamReader, ZstdShuffleDecompressor> reader(myFile);
            QdataUnserializer<BlockCompressReader<IfStreamReader, ZstdShuffleDecompressor>> unserializer(reader, use_alt_rep);
            return unserializer.read_object();
        } else {
            BlockCompressReader<IfStreamReader, ZstdDecompressor> reader(myFile);
            QdataUnserializer<BlockCompressReader<IfStreamReader, ZstdDecompressor>> unserializer(reader, use_alt_rep);
            return unserializer.read_object();
        }
    }
}

