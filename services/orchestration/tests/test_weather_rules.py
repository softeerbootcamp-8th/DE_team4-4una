# jobs/weather_rules.py 테스트 (#215).

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.weather_rules import (
    ICE,
    LOW_VISIBILITY,
    RAIN,
    SNOW,
    WEATHER_RULE_VERSION,
    WIND,
    adjust_comfort_scores,
    build_impact_signature,
    classify_weather,
    format_impact_signature,
    load_weather_rule_config,
    parse_impact_signature,
    weather_deduction,
)

CONFIG = load_weather_rule_config()

# 15분 합으로 들어오는 값이므로 시간당 기준을 4로 나눠 경계를 만든다.
RAIN_AT_THRESHOLD = CONFIG.rain_mm_per_hour.value / 4
SNOW_AT_THRESHOLD = CONFIG.snow_cm_per_hour.value / 4


def reading(**overrides) -> dict:
    values = {
        "temperature_2m": 20.0,
        "precipitation": 0.0,
        "rain": 0.0,
        "snowfall": 0.0,
        "visibility": 10000.0,
        "wind_speed_10m": 3.0,
        "wind_gusts_10m": 5.0,
        "weather_code": 0,
    }
    values.update(overrides)
    return values


class TestLoadWeatherRuleConfig:
    def test_weights_sum_to_one(self):
        total = (
            CONFIG.vertical_weight.value
            + CONFIG.longitudinal_weight.value
            + CONFIG.lateral_weight.value
        )

        assert total == pytest.approx(1.0)

    def test_rejects_weights_that_do_not_sum_to_one(self, tmp_path):
        source = (
            Path(__file__).resolve().parents[1] / "jobs" / "resources" / "weather_rules.yaml"
        ).read_text()
        broken = tmp_path / "weather_rules.yaml"
        broken.write_text(source.replace("vertical_weight:\n  value: 0.5", "vertical_weight:\n  value: 0.6"))

        with pytest.raises(ValueError, match="must sum to 1.0"):
            load_weather_rule_config(broken)


class TestClassifyWeather:
    @pytest.mark.parametrize(
        ("overrides", "expected"),
        [
            ({}, frozenset()),
            ({"rain": RAIN_AT_THRESHOLD}, {RAIN}),
            ({"rain": RAIN_AT_THRESHOLD * 0.5}, frozenset()),
            ({"snowfall": SNOW_AT_THRESHOLD}, {SNOW}),
            # 실측이 0이어도 WMO 코드가 강수를 말하면 인정한다.
            ({"weather_code": 61}, {RAIN}),
            ({"weather_code": 73}, {SNOW}),
            ({"weather_code": 66}, {RAIN, ICE}),
            ({"wind_gusts_10m": CONFIG.wind_gust_mps.value}, {WIND}),
            ({"visibility": CONFIG.low_visibility_m.value}, {LOW_VISIBILITY}),
            # 안개 코드는 실측 시야가 좋아도 불량으로 본다.
            ({"weather_code": 45}, {LOW_VISIBILITY}),
            # 음수는 관측 오류라 없음으로 취급한다.
            ({"rain": -1.0, "snowfall": -1.0}, frozenset()),
        ],
    )
    def test_conditions(self, overrides, expected):
        assert classify_weather(reading(**overrides), CONFIG) == frozenset(expected)

    def test_freezing_temperature_without_precipitation_is_not_ice(self):
        cold = reading(temperature_2m=CONFIG.freezing_temperature_c.value)

        assert ICE not in classify_weather(cold, CONFIG)

    def test_freezing_temperature_with_rain_is_ice(self):
        freezing_rain = reading(temperature_2m=CONFIG.freezing_temperature_c.value, rain=0.5)

        assert ICE in classify_weather(freezing_rain, CONFIG)

    def test_missing_temperature_is_not_ice(self):
        # None을 0으로 치환하면 기온 결측이 곧 결빙이 된다.
        assert ICE not in classify_weather(reading(temperature_2m=None, rain=0.5), CONFIG)

    def test_conditions_are_independent(self):
        conditions = classify_weather(
            reading(snowfall=1.0, wind_gusts_10m=20.0, visibility=500.0), CONFIG
        )

        assert conditions == {SNOW, WIND, LOW_VISIBILITY}

    def test_missing_fields_are_absent_not_an_error(self):
        assert classify_weather({}, CONFIG) == frozenset()


class TestImpactSignature:
    def test_clear_weather_has_its_own_body(self):
        assert build_impact_signature(reading(), CONFIG) == f"{WEATHER_RULE_VERSION}|clear"

    def test_conditions_are_sorted_so_the_signature_is_stable(self):
        assert format_impact_signature(frozenset({SNOW, ICE})) == f"{WEATHER_RULE_VERSION}|ice,snow"

    def test_a_change_within_the_same_conditions_keeps_the_signature(self):
        # 예전 구현은 raw 관측값 해시라 이 두 값이 다른 서명을 만들었고, 그래서 15분마다
        # 모든 zone이 변경으로 판정됐다.
        assert build_impact_signature(reading(temperature_2m=21.4), CONFIG) == (
            build_impact_signature(reading(temperature_2m=24.9), CONFIG)
        )

    def test_round_trips_through_parse(self):
        conditions = classify_weather(reading(rain=1.0, wind_gusts_10m=20.0), CONFIG)

        assert parse_impact_signature(format_impact_signature(conditions)) == conditions

    def test_parses_clear_weather(self):
        assert parse_impact_signature(f"{WEATHER_RULE_VERSION}|clear") == frozenset()

    def test_rejects_another_rule_version(self):
        with pytest.raises(ValueError, match="rule version"):
            parse_impact_signature("0.9.0|clear")

    def test_rejects_an_unknown_condition(self):
        with pytest.raises(ValueError, match="unknown condition"):
            parse_impact_signature(f"{WEATHER_RULE_VERSION}|hail")


class TestWeatherDeduction:
    @pytest.mark.parametrize(
        ("conditions", "expected"),
        [
            (frozenset(), (0.0, 0.0, 0.0, 0.0)),
            ({RAIN}, (0.0, CONFIG.rain_longitudinal_deduction.value, 0.0, 0.0)),
            ({ICE}, (0.0, CONFIG.ice_longitudinal_deduction.value, 0.0, 0.0)),
            (
                {SNOW},
                (
                    CONFIG.snow_vertical_deduction.value,
                    CONFIG.snow_longitudinal_deduction.value,
                    0.0,
                    0.0,
                ),
            ),
            ({WIND}, (0.0, 0.0, CONFIG.wind_lateral_deduction.value, 0.0)),
            ({LOW_VISIBILITY}, (0.0, 0.0, 0.0, CONFIG.low_visibility_final_deduction.value)),
        ],
    )
    def test_direction_mapping(self, conditions, expected):
        deduction = weather_deduction(frozenset(conditions), CONFIG)

        assert (
            deduction.vertical,
            deduction.longitudinal,
            deduction.lateral,
            deduction.final,
        ) == expected

    def test_overlapping_conditions_take_the_maximum_not_the_sum(self):
        deduction = weather_deduction(frozenset({RAIN, ICE, SNOW}), CONFIG)

        assert deduction.longitudinal == max(
            CONFIG.rain_longitudinal_deduction.value,
            CONFIG.ice_longitudinal_deduction.value,
            CONFIG.snow_longitudinal_deduction.value,
        )


class TestAdjustComfortScores:
    def test_clear_weather_leaves_the_standard_scores_untouched(self):
        adjusted = adjust_comfort_scores(80.0, 70.0, 60.0, frozenset(), CONFIG)

        assert (adjusted.vertical_score, adjusted.longitudinal_score, adjusted.lateral_score) == (
            80.0,
            70.0,
            60.0,
        )
        assert adjusted.comfort_score == pytest.approx(0.5 * 80 + 0.3 * 70 + 0.2 * 60)

    def test_deducts_per_direction_then_recombines(self):
        adjusted = adjust_comfort_scores(80.0, 70.0, 60.0, frozenset({RAIN, WIND}), CONFIG)

        longitudinal = 70.0 - CONFIG.rain_longitudinal_deduction.value
        lateral = 60.0 - CONFIG.wind_lateral_deduction.value
        assert (adjusted.longitudinal_score, adjusted.lateral_score) == (longitudinal, lateral)
        assert adjusted.comfort_score == pytest.approx(
            0.5 * 80.0 + 0.3 * longitudinal + 0.2 * lateral
        )

    def test_low_visibility_only_moves_the_final_score(self):
        adjusted = adjust_comfort_scores(80.0, 70.0, 60.0, frozenset({LOW_VISIBILITY}), CONFIG)

        # 방향 점수는 그대로고 결합값만 깎이므로 가중합 항등식이 깨진다.
        assert (adjusted.vertical_score, adjusted.longitudinal_score, adjusted.lateral_score) == (
            80.0,
            70.0,
            60.0,
        )
        assert adjusted.comfort_score == pytest.approx(
            0.5 * 80 + 0.3 * 70 + 0.2 * 60 - CONFIG.low_visibility_final_deduction.value
        )

    def test_stays_within_zero_and_one_hundred(self):
        worst = frozenset({RAIN, ICE, SNOW, WIND, LOW_VISIBILITY})

        assert adjust_comfort_scores(1.0, 1.0, 1.0, worst, CONFIG).comfort_score == 0.0
        assert adjust_comfort_scores(100.0, 100.0, 100.0, frozenset(), CONFIG).comfort_score == 100.0

    @pytest.mark.parametrize(
        "conditions", [{RAIN}, {ICE}, {SNOW}, {WIND}, {LOW_VISIBILITY}, {SNOW, WIND}]
    )
    def test_a_condition_never_improves_comfort(self, conditions):
        # comfort-score.md "Evaluation strategy"의 단조성 불변식.
        clear = adjust_comfort_scores(80.0, 70.0, 60.0, frozenset(), CONFIG).comfort_score

        assert adjust_comfort_scores(80.0, 70.0, 60.0, frozenset(conditions), CONFIG).comfort_score <= clear
