package s2s.query_compiler.template;


import s2s.engine.*;
import s2s.engine.*;
import com.google.common.util.concurrent.AtomicDouble;
import s2s.engine.Date;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.*;

import static s2s.query_compiler.template.CodeTemplateBasedVisitor.*;
import static s2s.query_compiler.template.CodeTemplateUtils.*;

public class CodegenContext {

    private final List<Class<?>> imports = new LinkedList<>();
    private final HashMap<String, Integer> names = new HashMap<>();
    private final HashMap<String, String> innerClasses = new HashMap<>();
    private final HashMap<String, String> methods = new HashMap<>();
    private final LinkedList<Declaration> classFields = new LinkedList<>();

    {
        List<Class<?>> importClasses = Arrays.asList(
                Objects.class,
                HashSet.class,
                Arrays.class,
                List.class,
                ArrayList.class,
                Date.class,
                Collection.class,
                Collections.class,
                Comparator.class,
                Collector.class,
                Collectors.class,

                Stream.class,
                DoubleStream.class,
                IntStream.class,
                LongStream.class,
                Consumer.class,

                LongAdder.class,
                AtomicLong.class,
                DoubleAdder.class,
                AtomicDouble.class,
                DoubleAccumulator.class,

                StringMultiContains.class,
                MarkedArrayList.class,
                FinalHashMap.class,
                FinalConcurrentHashMap.class,
                ConcurrentLinkedQueue.class,
                S2SCollectors.class,
                EmptyArrayList.class
        );
        imports.addAll(importClasses);
    }

    public String freshName(String prefix) {
        Integer occurrences = names.getOrDefault(prefix, 0);
        String name = prefix + "_" + occurrences;
        names.put(prefix, occurrences + 1);
        return name;
    }

    public void addInnerClass(String name, String code) {
        if(innerClasses.containsKey(name)) {
            throw new RuntimeException("Inner class name already used");
        }
        // TODO indent in a better way
        code = "\t" + code.replace(EOL, EOL_TAB);
        innerClasses.put(name, code);
    }


    public void addMethodFixName(String name, String code) {
        if(methods.containsKey(name)) {
            throw new RuntimeException("Method name already used");
        }
        // TODO indent in a better way
        //code = "\t" + code.replace("\n", "\n\t");
        code = "\t" + code;
        methods.put(name, code);
    }
    public String addClassField(String name, Class<?> type, String code) {
        String fresh = freshName(name);
        imports.add(type);
        classFields.add(new Declaration(fresh, type.getSimpleName(), code));
        return fresh;
    }
    public String addClassField(String name, String typeName, String code) {
        String fresh = freshName(name);
        classFields.add(new Declaration(fresh, typeName, code));
        return fresh;
    }


    public String asClass(String name, String packageName, String[] imports, String parent, String[] interfaces) {
        // TODO improve indentation here
        if(imports == null) imports = new String[]{};
        Stream<String> thisImports = this.imports.stream().map(Class::getCanonicalName);
        String allImports = Stream.concat(thisImports, Arrays.stream(imports))
                .map("import %s;"::formatted)
                .distinct()
                .collect(Collectors.joining(EOL));
        String extendsStr = parent != null ? "extends " + parent : "";
        String implementsStr = interfaces != null ? "implements " + String.join(",", interfaces) : "";
        String classFieldsStr = classFields.stream()
                .map(declaration -> "\tstatic final " + declaration.toCode())
                .collect(Collectors.joining(EOL));
        String innerClassesStr = String.join(EOL+EOL, innerClasses.values());
        String methodsStr = indent(String.join(EOL, methods.values()));

        return """
                package %s;
                
                %s;
                
                public class %s %s %s {
                    // class fields
                %s
                
                    // inner classes
                %s
                
                    // methods
                %s
                }
                """
                .formatted(
                        packageName,
                        allImports,
                        name,
                        extendsStr,
                        implementsStr,
                        classFieldsStr,
                        innerClassesStr,
                        methodsStr
                );
    }



}
