import math
from datetime import UTC, datetime

import pytest

from fraud_infer.features import (
    encode_channel,
    encode_event_created_at,
    encode_gender,
    encode_income,
    encode_location,
    encode_subscription,
)


class TestEncodeGender:
    def test_male(self):
        assert encode_gender("male") == 1

    def test_female(self):
        assert encode_gender("female") == 2

    def test_unknown_defaults_to_zero(self):
        assert encode_gender("unknown") == 0
        assert encode_gender("other") == 0
        assert encode_gender("") == 0

    def test_case_insensitive(self):
        assert encode_gender("MALE") == 1
        assert encode_gender("Female") == 2

    def test_whitespace_stripped(self):
        assert encode_gender("  male  ") == 1


class TestEncodeSubscription:
    def test_all_values(self):
        assert encode_subscription("FREE") == 0
        assert encode_subscription("STANDARD") == 1
        assert encode_subscription("PREMIUM") == 2

    def test_case_insensitive(self):
        assert encode_subscription("free") == 0

    def test_unknown_raises(self):
        with pytest.raises(KeyError):
            encode_subscription("GOLD")


class TestEncodeIncome:
    def test_all_values(self):
        assert encode_income("LOW") == 0
        assert encode_income("MED") == 1
        assert encode_income("HIGH") == 2

    def test_unknown_raises(self):
        with pytest.raises(KeyError):
            encode_income("ULTRA")


class TestEncodeChannel:
    def test_online(self):
        assert encode_channel("ONLINE") == 0

    def test_in_store(self):
        assert encode_channel("IN-STORE") == 1

    def test_unknown_raises(self):
        with pytest.raises(KeyError):
            encode_channel("MOBILE")


class TestEncodeEventCreatedAt:
    def test_float_passthrough(self):
        assert encode_event_created_at(1_720_000_000.0) == 1_720_000_000.0

    def test_aware_datetime(self):
        dt = datetime(2024, 7, 3, 10, 0, 0, tzinfo=UTC)
        assert encode_event_created_at(dt) == pytest.approx(dt.timestamp())

    def test_naive_datetime_treated_as_utc(self):
        naive = datetime(2024, 7, 3, 10, 0, 0)
        aware = datetime(2024, 7, 3, 10, 0, 0, tzinfo=UTC)
        assert encode_event_created_at(naive) == pytest.approx(aware.timestamp())

    def test_none_returns_nan(self):
        assert math.isnan(encode_event_created_at(None))


class TestEncodeLocation:
    def test_numeric_string(self):
        assert encode_location("42") == 42

    def test_non_numeric_string_returns_nan(self):
        assert math.isnan(encode_location("Sydney"))
        assert math.isnan(encode_location("Melbourne"))
