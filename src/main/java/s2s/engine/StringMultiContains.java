package s2s.engine;

public final class StringMultiContains {

    private final String[] substrings;

    public StringMultiContains(String[] substrings) {
        this.substrings = substrings;
    }
    public boolean match(String input) {
        return input.length() >= substrings.length && runBooleanWithString(substrings, input);
    }
    public static boolean match(String[] substrings, String input) {
        return input.length() >= substrings.length && runBooleanWithString(substrings, input);
    }

    private static boolean runBooleanWithString(String[] substrings, String string) {
        int start = 0;
        for(String sub : substrings) {
            int find = string.indexOf(sub, start);
            if(find == -1) {
                return false;
            }
            start = find + sub.length();
        }
        return true;
    }
}