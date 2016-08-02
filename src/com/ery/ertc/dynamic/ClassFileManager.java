package com.ery.ertc.dynamic;

import java.io.IOException;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;

@SuppressWarnings("rawtypes")
public class ClassFileManager extends ForwardingJavaFileManager {

    private JavaClassObject jclassObject;

    public JavaClassObject getJavaClassObject() {
        return jclassObject;
    }

    @SuppressWarnings("unchecked")
	public ClassFileManager(StandardJavaFileManager standardManager) {
        super(standardManager);
    }


    @Override
    public JavaFileObject getJavaFileForOutput(Location location,
        String className, JavaFileObject.Kind kind, FileObject sibling)
            throws IOException {
            jclassObject = new JavaClassObject(className, kind);
        return jclassObject;
    }
}