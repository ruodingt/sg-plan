import pytest

from fraud_infer.features import (
    encode_channel,
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


class TestEncodeLocation:
    def test_numeric_string(self):
        assert encode_location("42") == 42

    def test_string_is_deterministic(self):
        assert encode_location("Sydney") == encode_location("Sydney")

    def test_string_returns_non_negative(self):
        result = encode_location("Melbourne")
        assert isinstance(result, int)
        assert result >= 0

    def test_different_strings_differ(self):
        assert encode_location("Sydney") != encode_location("Melbourne")
