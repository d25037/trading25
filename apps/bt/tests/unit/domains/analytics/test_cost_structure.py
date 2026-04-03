from __future__ import annotations

import pytest

from src.domains.analytics.cost_structure import (
    CostStructurePoint,
    CostStructureStatement,
    _append_warning,
    _build_derived_point,
    build_fiscal_year_cost_structure_points,
    _build_operating_margin,
    _filter_warnings_for_selected_points,
    _normalize_statement,
    _select_cost_structure_points,
    _StatementCycle,
    analyze_cost_structure,
    build_statement_cycles,
    calculate_cost_structure_regression,
    normalize_cost_structure_points,
)


def make_statement(
    disclosed_date: str,
    period_type: str,
    sales: float | None,
    operating_profit: float | None,
) -> CostStructureStatement:
    return CostStructureStatement(
        disclosed_date=disclosed_date,
        period_type=period_type,
        sales=sales,
        operating_profit=operating_profit,
    )


class TestBuildStatementCycles:
    def test_invalid_period_rows_are_ignored(self) -> None:
        cycles = build_statement_cycles(
            [
                make_statement("2024-08-09", "INVALID", 1_000, 100),
                make_statement("2024-11-08", "2Q", 2_500, 280),
            ]
        )

        assert len(cycles) == 1
        assert cycles[0].fiscal_year == "2025"
        assert [statement.period_type for statement in cycles[0].statements] == ["2Q"]

    def test_returns_empty_when_all_rows_are_invalid(self) -> None:
        assert build_statement_cycles([make_statement("2024-08-09", "INVALID", 1_000, 100)]) == []

    def test_trailing_incomplete_cycle_is_grouped_after_latest_fy(self) -> None:
        cycles = build_statement_cycles(
            [
                make_statement("2024-05-10", "FY", 10_000, 1_000),
                make_statement("2024-08-09", "1Q", 3_000, 300),
            ]
        )

        assert len(cycles) == 2
        assert cycles[0].fiscal_year == "2024"
        assert cycles[0].closed is True
        assert cycles[0].left_censored is True
        assert cycles[1].fiscal_year == "2025"
        assert cycles[1].closed is False
        assert cycles[1].left_censored is False

    def test_consecutive_fy_disclosures_merge_into_same_cycle(self) -> None:
        cycles = build_statement_cycles(
            [
                make_statement("2020-08-06", "1Q", 4_600, 14),
                make_statement("2020-11-06", "2Q", 11_375, 520),
                make_statement("2021-02-10", "3Q", 19_525, 1_508),
                make_statement("2021-05-12", "FY", 27_214, 2_197),
                make_statement("2021-05-31", "FY", 27_214, 2_197),
                make_statement("2021-08-04", "1Q", 7_936, 997),
            ]
        )

        assert len(cycles) == 2
        assert cycles[0].fiscal_year == "2021"
        assert [statement.period_type for statement in cycles[0].statements] == ["1Q", "2Q", "3Q", "FY", "FY"]
        assert cycles[1].fiscal_year == "2022"


class TestNormalizeCostStructurePoints:
    def test_normalizes_single_quarter_points_from_cumulative_rows(self) -> None:
        points, warnings = normalize_cost_structure_points(
            [
                make_statement("2024-08-09", "1Q", 1_000, 100),
                make_statement("2024-11-08", "2Q", 2_500, 280),
                make_statement("2025-02-07", "3Q", 4_500, 540),
                make_statement("2025-05-09", "FY", 6_000, 700),
            ]
        )

        assert warnings == []
        assert [(point.analysis_period_type, point.sales, point.operating_profit) for point in points] == [
            ("1Q", 1_000, 100),
            ("2Q", 1_500, 180),
            ("3Q", 2_000, 260),
            ("4Q", 1_500, 160),
        ]
        assert [point.is_derived for point in points] == [False, True, True, True]

    def test_missing_predecessor_skips_point_with_warning(self) -> None:
        points, warnings = normalize_cost_structure_points(
            [
                make_statement("2024-11-08", "2Q", 2_500, 280),
                make_statement("2025-02-07", "3Q", 4_500, 540),
            ]
        )

        assert [(point.analysis_period_type, point.sales, point.operating_profit) for point in points] == [
            ("3Q", 2_000, 260),
        ]
        assert warnings == ["Skipped 2025 2Q: missing valid 1Q predecessor for single-quarter normalization."]

    def test_duplicate_disclosures_keep_latest_statement_for_period(self) -> None:
        points, warnings = normalize_cost_structure_points(
            [
                make_statement("2024-08-09", "1Q", 1_000, 100),
                make_statement("2024-10-01", "1Q", 1_200, 120),
                make_statement("2024-11-08", "2Q", 2_700, 330),
                make_statement("2025-02-07", "3Q", 4_800, 570),
                make_statement("2025-05-09", "FY", 6_300, 750),
            ]
        )

        assert warnings == []
        assert [(point.analysis_period_type, point.sales, point.operating_profit) for point in points] == [
            ("1Q", 1_200, 120),
            ("2Q", 1_500, 210),
            ("3Q", 2_100, 240),
            ("4Q", 1_500, 180),
        ]

    def test_duplicate_disclosures_prefer_latest_usable_statement(self) -> None:
        points, warnings = normalize_cost_structure_points(
            [
                make_statement("2024-08-09", "1Q", 1_000, 100),
                make_statement("2024-11-08", "2Q", 2_500, 280),
                make_statement("2025-02-06", "3Q", 4_400, 520),
                make_statement("2025-02-07", "3Q", 4_400, None),
                make_statement("2025-05-09", "FY", 6_000, 700),
            ]
        )

        assert warnings == []
        assert [(point.analysis_period_type, point.sales, point.operating_profit) for point in points] == [
            ("1Q", 1_000, 100),
            ("2Q", 1_500, 180),
            ("3Q", 1_900, 240),
            ("4Q", 1_600, 180),
        ]

    def test_invalid_rows_are_skipped(self) -> None:
        points, warnings = normalize_cost_structure_points(
            [
                make_statement("2024-08-09", "1Q", None, 100),
                make_statement("2024-11-08", "2Q", 2_500, 280),
                make_statement("2025-02-07", "3Q", 4_500, None),
                make_statement("2025-05-09", "FY", 6_000, 700),
            ]
        )

        assert points == []
        assert warnings == [
            "Skipped 2025 1Q: sales was missing, non-finite, or non-positive.",
            "Skipped 2025 3Q: operating profit was missing or non-finite.",
            "Skipped 2025 2Q: missing valid 1Q predecessor for single-quarter normalization.",
            "Skipped 2025 4Q: missing valid 3Q predecessor for single-quarter normalization.",
        ]

    def test_left_censored_leading_fy_does_not_emit_fake_missing_3q_warning(self) -> None:
        points, warnings = normalize_cost_structure_points(
            [
                make_statement("2016-05-11", "FY", 28_403, 2_853),
                make_statement("2016-08-04", "1Q", 6_589, 642),
                make_statement("2016-11-08", "2Q", 13_070, 1_116),
                make_statement("2017-02-06", "3Q", 20_154, 1_555),
                make_statement("2017-05-10", "FY", 27_597, 1_994),
            ]
        )

        assert "Skipped 2016 4Q: missing valid 3Q predecessor for single-quarter normalization." not in warnings
        assert [(point.fiscal_year, point.analysis_period_type) for point in points] == [
            ("2017", "1Q"),
            ("2017", "2Q"),
            ("2017", "3Q"),
            ("2017", "4Q"),
        ]

    def test_duplicate_fy_disclosure_does_not_emit_fake_missing_3q_warning(self) -> None:
        points, warnings = normalize_cost_structure_points(
            [
                make_statement("2020-08-06", "1Q", 4_600, 14),
                make_statement("2020-11-06", "2Q", 11_375, 520),
                make_statement("2021-02-10", "3Q", 19_525, 1_508),
                make_statement("2021-05-12", "FY", 27_214, 2_197),
                make_statement("2021-05-31", "FY", 27_214, 2_197),
                make_statement("2021-08-04", "1Q", 7_936, 997),
            ]
        )

        assert "Skipped 2021 4Q: missing valid 3Q predecessor for single-quarter normalization." not in warnings
        assert [(point.fiscal_year, point.analysis_period_type) for point in points] == [
            ("2021", "1Q"),
            ("2021", "2Q"),
            ("2021", "3Q"),
            ("2021", "4Q"),
            ("2022", "1Q"),
        ]

    def test_builds_fiscal_year_cumulative_points(self) -> None:
        points, warnings = build_fiscal_year_cost_structure_points(
            [
                make_statement("2022-08-01", "1Q", 1_000, 100),
                make_statement("2022-11-01", "2Q", 2_100, 220),
                make_statement("2023-02-01", "3Q", 3_300, 360),
                make_statement("2023-05-01", "FY", 4_600, 490),
                make_statement("2023-08-01", "1Q", 1_200, 140),
                make_statement("2023-11-01", "2Q", 2_400, 300),
                make_statement("2024-02-01", "3Q", 3_700, 510),
                make_statement("2024-05-01", "FY", 5_000, 720),
            ]
        )

        assert warnings == []
        assert [(point.analysis_period_type, point.sales, point.operating_profit) for point in points] == [
            ("FY", 4_600, 490),
            ("FY", 5_000, 720),
        ]


class TestCostStructureHelpers:
    def test_append_warning_deduplicates_messages(self) -> None:
        warnings = ["duplicate"]

        _append_warning(warnings, "duplicate")
        _append_warning(warnings, "new")

        assert warnings == ["duplicate", "new"]

    def test_normalize_statement_rejects_unknown_period(self) -> None:
        assert _normalize_statement(make_statement("2024-08-09", "INVALID", 1_000, 100)) is None

    def test_build_operating_margin_returns_none_for_invalid_sales(self) -> None:
        assert _build_operating_margin(0, 100) is None

    def test_build_derived_point_rejects_non_positive_sales(self) -> None:
        cycle = _StatementCycle(fiscal_year="2025", statements=[], closed=True, left_censored=False)
        warnings: list[str] = []

        point = _build_derived_point(
            cycle,
            make_statement("2025-05-09", "FY", 2_000, 300),
            make_statement("2025-02-07", "3Q", 2_000, 200),
            "4Q",
            warnings,
        )

        assert point is None
        assert warnings == ["Skipped 2025 4Q: normalized sales was non-finite or non-positive after cumulative diff."]

    def test_build_derived_point_rejects_non_finite_operating_profit(self) -> None:
        cycle = _StatementCycle(fiscal_year="2025", statements=[], closed=True, left_censored=False)
        warnings: list[str] = []

        point = _build_derived_point(
            cycle,
            make_statement("2025-05-09", "FY", 3_000, float("inf")),
            make_statement("2025-02-07", "3Q", 2_000, 200),
            "4Q",
            warnings,
        )

        assert point is None
        assert warnings == ["Skipped 2025 4Q: normalized operating profit was non-finite after cumulative diff."]

    def test_select_cost_structure_points_supports_all_view(self) -> None:
        points = [
            CostStructurePoint("2024-01-01", "2024-01-01", "2024", "1Q", 100, 10, 10.0, False),
            CostStructurePoint("2024-04-01", "2024-04-01", "2024", "2Q", 200, 30, 15.0, True),
            CostStructurePoint("2024-07-01", "2024-07-01", "2024", "3Q", 300, 70, 23.3, True),
        ]

        assert _select_cost_structure_points(points, view="all", window_quarters=12) == points

    def test_select_cost_structure_points_raises_for_empty_same_quarter(self) -> None:
        with pytest.raises(ValueError, match="same_quarter view"):
            _select_cost_structure_points([], view="same_quarter", window_quarters=12)

    def test_filter_warnings_for_selected_points_filters_by_period_and_year(self) -> None:
        points = [CostStructurePoint("2024-07-01", "2024-07-01", "2024", "3Q", 300, 70, 23.3, True)]

        filtered = _filter_warnings_for_selected_points(
            [
                "Skipped 2024 3Q: missing valid 2Q predecessor for single-quarter normalization.",
                "Skipped 2024 4Q: missing valid 3Q predecessor for single-quarter normalization.",
                "Generic warning",
            ],
            points,
        )

        assert filtered == [
            "Skipped 2024 3Q: missing valid 2Q predecessor for single-quarter normalization.",
            "Generic warning",
        ]

    def test_filter_warnings_for_selected_points_keeps_fy_warnings(self) -> None:
        points = [CostStructurePoint("2024-05-01", "2024-05-01", "2024", "FY", 5_000, 720, 14.4, False)]

        filtered = _filter_warnings_for_selected_points(
            [
                "Skipped 2024 FY: sales was missing, non-finite, or non-positive.",
                "Skipped 2023 FY: sales was missing, non-finite, or non-positive.",
            ],
            points,
        )

        assert filtered == ["Skipped 2024 FY: sales was missing, non-finite, or non-positive."]


class TestCalculateCostStructureRegression:
    def test_returns_expected_regression_fields(self) -> None:
        regression = calculate_cost_structure_regression(
            [
                CostStructurePoint("2024-01-01", "2024-01-01", "2024", "1Q", 100, -10, -10.0, False),
                CostStructurePoint("2024-04-01", "2024-04-01", "2024", "2Q", 200, 30, 15.0, True),
                CostStructurePoint("2024-07-01", "2024-07-01", "2024", "3Q", 300, 70, 23.333333, True),
            ]
        )

        assert regression.sample_count == 3
        assert regression.slope == pytest.approx(0.4)
        assert regression.intercept == pytest.approx(-50.0)
        assert regression.r_squared == pytest.approx(1.0)
        assert regression.contribution_margin_ratio == pytest.approx(0.4)
        assert regression.variable_cost_ratio == pytest.approx(0.6)
        assert regression.fixed_cost == pytest.approx(50.0)
        assert regression.break_even_sales == pytest.approx(125.0)
        assert regression.warnings == []

    def test_non_interpretable_fixed_cost_and_break_even_return_null_with_warning(self) -> None:
        regression = calculate_cost_structure_regression(
            [
                CostStructurePoint("2024-01-01", "2024-01-01", "2024", "1Q", 100, 90, 90.0, False),
                CostStructurePoint("2024-04-01", "2024-04-01", "2024", "2Q", 200, 130, 65.0, True),
                CostStructurePoint("2024-07-01", "2024-07-01", "2024", "3Q", 300, 170, 56.666667, True),
            ]
        )

        assert regression.slope == pytest.approx(0.4)
        assert regression.intercept == pytest.approx(50.0)
        assert regression.fixed_cost is None
        assert regression.break_even_sales is None
        assert regression.warnings == [
            "Fixed cost could not be interpreted because regression intercept was non-negative.",
            "Break-even sales could not be interpreted because fixed cost was not economically interpretable.",
        ]

    def test_requires_three_points(self) -> None:
        with pytest.raises(ValueError, match="minimum 3"):
            calculate_cost_structure_regression(
                [
                    CostStructurePoint("2024-01-01", "2024-01-01", "2024", "1Q", 100, 10, 10.0, False),
                    CostStructurePoint("2024-04-01", "2024-04-01", "2024", "2Q", 200, 40, 20.0, True),
                ]
            )

    def test_negative_slope_warns_that_break_even_is_not_interpretable(self) -> None:
        regression = calculate_cost_structure_regression(
            [
                CostStructurePoint("2024-01-01", "2024-01-01", "2024", "1Q", 100, -20, -20.0, False),
                CostStructurePoint("2024-04-01", "2024-04-01", "2024", "2Q", 200, -25, -12.5, True),
                CostStructurePoint("2024-07-01", "2024-07-01", "2024", "3Q", 300, -30, -10.0, True),
            ]
        )

        assert regression.fixed_cost == pytest.approx(15.0)
        assert "Break-even sales could not be interpreted because contribution margin ratio was not positive." in regression.warnings


class TestAnalyzeCostStructure:
    def test_combines_normalization_and_regression(self) -> None:
        analysis = analyze_cost_structure(
            [
                make_statement("2024-08-09", "1Q", 1_000, 350),
                make_statement("2024-11-08", "2Q", 2_200, 820),
                make_statement("2025-02-07", "3Q", 3_600, 1_410),
                make_statement("2025-05-09", "FY", 4_700, 1_880),
            ]
        )

        assert analysis.latest_point.analysis_period_type == "4Q"
        assert analysis.date_from == "2024-08-09"
        assert analysis.date_to == "2025-05-09"
        assert analysis.regression.sample_count == 4

    def test_defaults_to_recent_twelve_quarters(self) -> None:
        statements: list[CostStructureStatement] = []
        for offset in range(4):
            fiscal_year = 2022 + offset
            base = 1_000 + offset * 100
            statements.extend(
                [
                    make_statement(f"{fiscal_year}-08-01", "1Q", base, base * 0.1),
                    make_statement(f"{fiscal_year}-11-01", "2Q", base * 2.1, base * 0.22),
                    make_statement(f"{fiscal_year + 1}-02-01", "3Q", base * 3.3, base * 0.36),
                    make_statement(f"{fiscal_year + 1}-05-01", "FY", base * 4.6, base * 0.49),
                ]
            )

        analysis = analyze_cost_structure(statements)

        assert len(analysis.points) == 12
        assert analysis.regression.sample_count == 12
        assert analysis.date_from == "2023-08-01"
        assert analysis.date_to == "2026-05-01"

    def test_same_quarter_view_filters_to_latest_quarter_type(self) -> None:
        analysis = analyze_cost_structure(
            [
                make_statement("2022-08-01", "1Q", 1_000, 100),
                make_statement("2022-11-01", "2Q", 2_100, 220),
                make_statement("2023-02-01", "3Q", 3_300, 360),
                make_statement("2023-05-01", "FY", 4_600, 490),
                make_statement("2023-08-01", "1Q", 1_200, 140),
                make_statement("2023-11-01", "2Q", 2_400, 300),
                make_statement("2024-02-01", "3Q", 3_700, 510),
                make_statement("2024-05-01", "FY", 5_000, 720),
                make_statement("2024-08-01", "1Q", 1_350, 180),
                make_statement("2024-11-01", "2Q", 2_700, 390),
                make_statement("2025-02-01", "3Q", 4_100, 690),
            ],
            view="same_quarter",
        )

        assert {point.analysis_period_type for point in analysis.points} == {"3Q"}
        assert analysis.regression.sample_count == 3

    def test_fiscal_year_only_view_uses_fiscal_year_cumulative_points(self) -> None:
        analysis = analyze_cost_structure(
            [
                make_statement("2022-08-01", "1Q", 1_000, 100),
                make_statement("2022-11-01", "2Q", 2_100, 220),
                make_statement("2023-02-01", "3Q", 3_300, 360),
                make_statement("2023-05-01", "FY", 4_600, 490),
                make_statement("2023-08-01", "1Q", 1_200, 140),
                make_statement("2023-11-01", "2Q", 2_400, 300),
                make_statement("2024-02-01", "3Q", 3_700, 510),
                make_statement("2024-05-01", "FY", 5_000, 720),
                make_statement("2024-08-01", "1Q", 1_350, 180),
                make_statement("2024-11-01", "2Q", 2_700, 390),
                make_statement("2025-02-01", "3Q", 4_100, 690),
                make_statement("2025-05-01", "FY", 5_500, 980),
            ],
            view="fiscal_year_only",
        )

        assert {point.analysis_period_type for point in analysis.points} == {"FY"}
        assert [point.sales for point in analysis.points] == [4_600, 5_000, 5_500]
        assert analysis.latest_point.analysis_period_type == "FY"
        assert analysis.latest_point.sales == 5_500
        assert analysis.regression.sample_count == 3
