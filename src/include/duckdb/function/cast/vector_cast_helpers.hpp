//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/cast/vector_cast_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/general_cast.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

template <class OP>
struct VectorStringCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto result = (Vector *)dataptr;
		return OP::template Operation<INPUT_TYPE>(input, *result);
	}
};

struct VectorTryCastData {
	VectorTryCastData(Vector &result_p, string *error_message_p, bool strict_p)
	    : result(result_p), error_message(error_message_p), strict(strict_p) {
	}

	Vector &result;
	string *error_message;
	bool strict;
	bool all_converted = true;
};

template <class OP>
struct VectorTryCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output))) {
			return output;
		}
		auto data = (VectorTryCastData *)dataptr;
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStrictOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastErrorOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(
		        OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->error_message, data->strict))) {
			return output;
		}
		bool has_error = data->error_message && !data->error_message->empty();
		return HandleVectorCastError::Operation<RESULT_TYPE>(
		    has_error ? *data->error_message : CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask, idx,
		    data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->result,
		                                                                  data->error_message, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

struct VectorDecimalCastData {
	// TODO good to hardcode decimal sep here?
	VectorDecimalCastData(string *error_message_p, uint8_t width_p, uint8_t scale_p, char decimal_separator = '.')
	    : error_message(error_message_p), width(width_p), scale(scale_p), decimal_separator(decimal_separator) {
	}

	string *error_message;
	uint8_t width;
	uint8_t scale;
	char decimal_separator;
	bool all_converted = true;
};

struct VectorStringToNumericCastData {
	VectorStringToNumericCastData(string *error_message_p, char decimal_separator, bool strict)
	    : error_message(error_message_p), decimal_separator(decimal_separator), strict(strict) {
	}

	string *error_message;
	char decimal_separator;
	bool strict = false;
	bool all_converted = true;
};

template <class OP>
struct VectorStringToDecimalCastOperator {
	// TODO: can this be specialized?
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorDecimalCastData *)dataptr;
		RESULT_TYPE result_value;
		if (!OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, result_value, data->error_message, data->width,
		                                                     data->scale, data->decimal_separator)) {
			return HandleVectorCastError::Operation<RESULT_TYPE>("Failed to cast decimal value", mask, idx,
			                                                     data->error_message, data->all_converted);
		}
		return result_value;
	}
};

template <class OP>
struct VectorDecimalCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorDecimalCastData *)dataptr;
		RESULT_TYPE result_value;
		if (!OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, result_value, data->error_message, data->width,
		                                                     data->scale)) {
			return HandleVectorCastError::Operation<RESULT_TYPE>("Failed to cast decimal value", mask, idx,
			                                                     data->error_message, data->all_converted);
		}
		return result_value;
	}
};

template <class OP>
struct VectorStringToNumericCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorStringToNumericCastData *)dataptr;
		RESULT_TYPE result_value;
		if (!OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, result_value, data->decimal_separator, data->strict)) {
			return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
		}
		return result_value;
	}
};

struct VectorCastHelpers {
	template <class SRC, class DST, class OP>
	static bool TemplatedCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		UnaryExecutor::Execute<SRC, DST, OP>(source, result, count);
		return true;
	}

	template <class SRC, class DST, class OP>
	static bool TemplatedTryCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		VectorTryCastData input(result, parameters.error_message, parameters.strict);
		UnaryExecutor::GenericExecute<SRC, DST, OP>(source, result, count, &input, parameters.error_message);
		return input.all_converted;
	}

	template <class SRC, class DST, class OP>
	static bool TryCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class DST, class OP>
	static bool TryCastStrictLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastStrictOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class DST, class OP>
	static bool TryCastErrorLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastErrorOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class DST, class OP>
	static bool TryCastStringLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastStringOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class OP>
	static bool StringCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		D_ASSERT(result.GetType().InternalType() == PhysicalType::VARCHAR);
		UnaryExecutor::GenericExecute<SRC, string_t, VectorStringCastOperator<OP>>(source, result, count,
		                                                                           (void *)&result);
		return true;
	}

	template <class SRC, class T, class OP>
	static bool TemplatedDecimalCast(Vector &source, Vector &result, idx_t count, string *error_message, uint8_t width,
	                                 uint8_t scale) {
		VectorDecimalCastData input(error_message, width, scale);
		UnaryExecutor::GenericExecute<SRC, T, VectorDecimalCastOperator<OP>>(source, result, count, (void *)&input,
		                                                                     error_message);
		return input.all_converted;
	}

	template <class SRC, class T, class OP>
	static bool TemplatedStringToDecimalCast(Vector &source, Vector &result, idx_t count, string *error_message, uint8_t width,
	                                 uint8_t scale, char decimal_separator) {
		VectorDecimalCastData input(error_message, width, scale, decimal_separator);
		UnaryExecutor::GenericExecute<SRC, T, VectorStringToDecimalCastOperator<OP>>(source, result, count, (void *)&input,
		                                                                     error_message);
		return input.all_converted;
	}

	template <class SRC, class T, class OP>
	static bool TemplatedStringToNumericCast(Vector &source, Vector &result, idx_t count, string *error_message, char decimal_separator, bool strict) {
		VectorStringToNumericCastData input(error_message, decimal_separator, strict);
		UnaryExecutor::GenericExecute<SRC, T, VectorStringToNumericCastOperator<OP>>(source, result, count, (void *)&input,
		                                                                     error_message);
		return input.all_converted;
	}

	template <class T>
	static bool ToDecimalCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &result_type = result.GetType();
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TemplatedDecimalCast<T, int16_t, TryCastToDecimal>(source, result, count, parameters.error_message,
			                                                          width, scale);
		case PhysicalType::INT32:
			return TemplatedDecimalCast<T, int32_t, TryCastToDecimal>(source, result, count, parameters.error_message,
			                                                          width, scale);
		case PhysicalType::INT64:
			return TemplatedDecimalCast<T, int64_t, TryCastToDecimal>(source, result, count, parameters.error_message,
			                                                          width, scale);
		case PhysicalType::INT128:
			return TemplatedDecimalCast<T, hugeint_t, TryCastToDecimal>(source, result, count, parameters.error_message,
			                                                            width, scale);
		default:
			throw InternalException("Unimplemented internal type for decimal");
		}
	}

	static bool StringToDecimalCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &result_type = result.GetType();
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TemplatedStringToDecimalCast<string_t, int16_t, TryCastStringToDecimal>(source, result, count, parameters.error_message,
			                                                          width, scale, parameters.decimal_separator);
		case PhysicalType::INT32:
			return TemplatedStringToDecimalCast<string_t, int32_t, TryCastStringToDecimal>(source, result, count, parameters.error_message,
			                                                          width, scale, parameters.decimal_separator);
		case PhysicalType::INT64:
			return TemplatedStringToDecimalCast<string_t, int64_t, TryCastStringToDecimal>(source, result, count, parameters.error_message,
			                                                          width, scale, parameters.decimal_separator);
		case PhysicalType::INT128:
			return TemplatedStringToDecimalCast<string_t, hugeint_t, TryCastStringToDecimal>(source, result, count, parameters.error_message,
			                                                            width, scale, parameters.decimal_separator);
		default:
			throw InternalException("Unimplemented internal type for decimal");
		}
	}

	template <class T, class DST>
	static bool StringToNumericCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedStringToNumericCast<T, DST, TryCastFromString>(source, result, count, parameters.error_message,
			                                                            parameters.decimal_separator, parameters.strict);
	}
};

struct VectorStringToList {
	static idx_t CountParts(const string_t &input);
	static bool SplitStringifiedList(const string_t &input, string_t *child_data, idx_t &child_start, Vector &child);
	static bool StringToNestedTypeCastLoop(string_t *source_data, ValidityMask &source_mask, Vector &result,
	                                       ValidityMask &result_mask, idx_t count, CastParameters &parameters,
	                                       const SelectionVector *sel);
};

struct VectorStringToStruct {
	static bool SplitStruct(string_t &input, std::vector<std::unique_ptr<Vector>> &varchar_vectors, idx_t &row_idx,
	                        string_map_t<idx_t> &child_names, std::vector<ValidityMask *> &child_masks);
	static bool StringToNestedTypeCastLoop(string_t *source_data, ValidityMask &source_mask, Vector &result,
	                                       ValidityMask &result_mask, idx_t count, CastParameters &parameters,
	                                       const SelectionVector *sel);
};

} // namespace duckdb
