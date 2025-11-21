package s2s.bench_generator;

import s2s.planner.calcite.CalcitePlanner;
import s2s.planner.qp.S2SPlan;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.Optional;

public record MaybePlannedQuery(String query, Optional<S2SPlan> plan) {

    public MaybePlannedQuery(String queryName) {
        this(queryName, Optional.empty());
    }

    public MaybePlannedQuery(String queryName, S2SPlan plan) {
        this(queryName, plan != null ? Optional.of(plan) : Optional.empty());
    }

    public PlannedQuery plan(CalcitePlanner planner) throws SqlParseException {
        return new PlannedQuery(query, plan.isPresent() ? plan.get() : planner.plan(query));
    }
}
