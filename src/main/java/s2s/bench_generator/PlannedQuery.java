package s2s.bench_generator;

import s2s.planner.qp.S2SPlan;

public record PlannedQuery(String query, S2SPlan plan) {}
