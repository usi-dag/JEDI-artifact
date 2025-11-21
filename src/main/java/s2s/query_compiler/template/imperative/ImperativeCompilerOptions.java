package s2s.query_compiler.template.imperative;


import s2s.query_compiler.options.CommonCompilerOption;
import s2s.query_compiler.options.SingleFieldTuplesConverter;

public class ImperativeCompilerOptions implements CommonCompilerOption {
    public static final ImperativeCompilerOptions DEFAULT = new ImperativeCompilerOptions();

    /**
     * Returns a string representation of the object.
     */
    public String toCompactString() {
        return "Opt";
    }

    @Override
    public SingleFieldTuplesConverter getSingleFieldTuplesConverter() {
        return SingleFieldTuplesConverter.TO_PRIMITIVE;
    }


}
