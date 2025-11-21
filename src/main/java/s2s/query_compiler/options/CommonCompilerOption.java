package s2s.query_compiler.options;

public interface CommonCompilerOption {

    String toCompactString();

    SingleFieldTuplesConverter getSingleFieldTuplesConverter();


}
