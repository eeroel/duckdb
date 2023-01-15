//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi/capi_cast_from_decimal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi/cast/utils.hpp"

namespace duckdb {

template <class INTERNAL_TYPE>
struct ToCDecimalCastWrapper {
	template <class SOURCE_TYPE>
	static bool Operation(SOURCE_TYPE input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Type not implemented for CDecimalCastWrapper");
	}
	static bool Operation(string_t input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale, char decimal_separator='.') {
		throw NotImplementedException("Type not implemented for CDecimalCastWrapper");
	}
};



//! Hugeint
template <>
struct ToCDecimalCastWrapper<hugeint_t> {
	template <class SOURCE_TYPE>
	static bool Operation(SOURCE_TYPE input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale) {
		hugeint_t intermediate_result;

		if (!TryCastToDecimal::Operation<SOURCE_TYPE, hugeint_t>(input, intermediate_result, error, width, scale)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = intermediate_result.upper;
		hugeint_value.lower = intermediate_result.lower;
		result.value = hugeint_value;
		return true;
	}

	static bool Operation(string_t input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale, char decimal_separator='.') {
		hugeint_t intermediate_result;

		if (!TryCastStringToDecimal::Operation<string_t, hugeint_t>(input, intermediate_result, error, width, scale, decimal_separator)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = intermediate_result.upper;
		hugeint_value.lower = intermediate_result.lower;
		result.value = hugeint_value;
		return true;
	}
};


//! FIXME: reduce duplication here by just matching on the signed-ness of the type
//! INTERNAL_TYPE = int16_t
template <>
struct ToCDecimalCastWrapper<int16_t> {
	template <class SOURCE_TYPE>
	static bool Operation(SOURCE_TYPE input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale) {
		int16_t intermediate_result;

		if (!TryCastToDecimal::Operation<SOURCE_TYPE, int16_t>(input, intermediate_result, error, width, scale)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		hugeint_t hugeint_result = Hugeint::Convert(intermediate_result);

		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = hugeint_result.upper;
		hugeint_value.lower = hugeint_result.lower;
		result.value = hugeint_value;
		return true;
	}
		static bool Operation(string_t input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale, char decimal_separator = '.') {
		int16_t intermediate_result;

		if (!TryCastStringToDecimal::Operation<string_t, int16_t>(input, intermediate_result, error, width, scale, decimal_separator)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		hugeint_t hugeint_result = Hugeint::Convert(intermediate_result);

		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = hugeint_result.upper;
		hugeint_value.lower = hugeint_result.lower;
		result.value = hugeint_value;
		return true;
	}
};


//! INTERNAL_TYPE = int32_t
template <>
struct ToCDecimalCastWrapper<int32_t> {
	template <class SOURCE_TYPE>
	static bool Operation(SOURCE_TYPE input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale) {
		int32_t intermediate_result;

		if (!TryCastToDecimal::Operation<SOURCE_TYPE, int32_t>(input, intermediate_result, error, width, scale)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		hugeint_t hugeint_result = Hugeint::Convert(intermediate_result);

		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = hugeint_result.upper;
		hugeint_value.lower = hugeint_result.lower;
		result.value = hugeint_value;
		return true;
	}

	static bool Operation(string_t input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale, char decimal_separator = '.') {
		int32_t intermediate_result;

		if (!TryCastStringToDecimal::Operation<string_t, int32_t>(input, intermediate_result, error, width, scale, decimal_separator)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		hugeint_t hugeint_result = Hugeint::Convert(intermediate_result);

		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = hugeint_result.upper;
		hugeint_value.lower = hugeint_result.lower;
		result.value = hugeint_value;
		return true;
	}
};

//! INTERNAL_TYPE = int64_t
template <>
struct ToCDecimalCastWrapper<int64_t> {
	template <class SOURCE_TYPE>
	static bool Operation(SOURCE_TYPE input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale) {
		int64_t intermediate_result;

		if (!TryCastToDecimal::Operation<SOURCE_TYPE, int64_t>(input, intermediate_result, error, width, scale)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		hugeint_t hugeint_result = Hugeint::Convert(intermediate_result);

		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = hugeint_result.upper;
		hugeint_value.lower = hugeint_result.lower;
		result.value = hugeint_value;
		return true;
	}

	static bool Operation(string_t input, duckdb_decimal &result, std::string *error, uint8_t width, uint8_t scale, char decimal_separator = '.') {
		int64_t intermediate_result;

		if (!TryCastStringToDecimal::Operation<string_t, int64_t>(input, intermediate_result, error, width, scale, decimal_separator)) {
			result = FetchDefaultValue::Operation<duckdb_decimal>();
			return false;
		}
		hugeint_t hugeint_result = Hugeint::Convert(intermediate_result);

		result.scale = scale;
		result.width = width;

		duckdb_hugeint hugeint_value;
		hugeint_value.upper = hugeint_result.upper;
		hugeint_value.lower = hugeint_result.lower;
		result.value = hugeint_value;
		return true;
	}
};

template <class SOURCE_TYPE, class OP>
duckdb_decimal TryCastToDecimalCInternal(SOURCE_TYPE source, uint8_t width, uint8_t scale) {
	duckdb_decimal result;
	try {
		if (!OP::template Operation<SOURCE_TYPE>(source, result, nullptr, width, scale)) {
			return FetchDefaultValue::Operation<duckdb_decimal>();
		}
	} catch (...) {
		return FetchDefaultValue::Operation<duckdb_decimal>();
	}
	return result;
}

template <class OP>
duckdb_decimal TryCastToDecimalCInternal(string_t source, uint8_t width, uint8_t scale, char decimal_separator = '.') {
	duckdb_decimal result;
	try {
		if (!OP::template Operation<string_t>(source, result, nullptr, width, scale, decimal_separator)) {
			return FetchDefaultValue::Operation<duckdb_decimal>();
		}
	} catch (...) {
		return FetchDefaultValue::Operation<duckdb_decimal>();
	}
	return result;
}

template <class SOURCE_TYPE, class OP>
duckdb_decimal TryCastToDecimalCInternal(duckdb_result *result, idx_t col, idx_t row, uint8_t width, uint8_t scale) {
	return TryCastToDecimalCInternal<SOURCE_TYPE, OP>(UnsafeFetch<SOURCE_TYPE>(result, col, row), width, scale);
}

template <class OP>
duckdb_decimal TryCastToDecimalCInternal(duckdb_result *result, idx_t col, idx_t row, uint8_t width, uint8_t scale, char decimal_separator = '.') {
	return TryCastToDecimalCInternal<OP>(UnsafeFetch<string_t>(result, col, row), width, scale, decimal_separator);
}

} // namespace duckdb
