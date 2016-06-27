/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.datanucleus.store.rdbms.datasource.dbcp.jocl;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import java.lang.reflect.InvocationTargetException;
import java.io.InputStream;
import java.io.Reader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

// to do:
//  + add support for strings as CDATA (makes multiline strings easier, for example)
//  ? some kind of support for invoking methods?

/**
 * A {@link org.xml.sax.ContentHandler}
 * for the Java Object Configuration Language.
 */
public class JOCLContentHandler extends DefaultHandler {

    //--- Static Methods ---------------------------------------------

    public static void main(String[] args) throws Exception {
        JOCLContentHandler jocl = JOCLContentHandler.parse(System.in,null);
        for(int i=0;i<jocl.size();i++) {
            System.out.println("<" + jocl.getType(i) + ">\t" + jocl.getValue(i));
        }
    }

    public static JOCLContentHandler parse(File f) throws SAXException, FileNotFoundException, IOException {
        return JOCLContentHandler.parse(new FileInputStream(f),null);
    }

    public static JOCLContentHandler parse(Reader in) throws SAXException, IOException {
        return JOCLContentHandler.parse(new InputSource(in),null);
    }

    public static JOCLContentHandler parse(InputStream in) throws SAXException, IOException {
        return JOCLContentHandler.parse(new InputSource(in),null);
    }

    public static JOCLContentHandler parse(InputSource in) throws SAXException, IOException {
        return JOCLContentHandler.parse(in,null);
    }

    public static JOCLContentHandler parse(File f, XMLReader reader) throws SAXException, FileNotFoundException, IOException {
        return JOCLContentHandler.parse(new FileInputStream(f),reader);
    }

    public static JOCLContentHandler parse(Reader in, XMLReader reader) throws SAXException, IOException {
        return JOCLContentHandler.parse(new InputSource(in),reader);
    }

    public static JOCLContentHandler parse(InputStream in, XMLReader reader) throws SAXException, IOException {
        return JOCLContentHandler.parse(new InputSource(in),reader);
    }

    public static JOCLContentHandler parse(InputSource in, XMLReader reader) throws SAXException, IOException {
        JOCLContentHandler jocl = new JOCLContentHandler();
        if(null == reader) {
            reader = XMLReaderFactory.createXMLReader();
        }
        reader.setContentHandler(jocl);
        reader.parse(in);
        return jocl;
    }

    public JOCLContentHandler() {
        this(true,true,true,true);
    }

    public JOCLContentHandler(boolean emptyEltNS, boolean joclEltPrefix, boolean emptyAttrNS, boolean joclAttrPrefix) {
        _acceptEmptyNamespaceForElements = emptyEltNS;
        _acceptJoclPrefixForElements = joclEltPrefix;
        _acceptEmptyNamespaceForAttributes = emptyAttrNS;
        _acceptJoclPrefixForAttributes = joclAttrPrefix;
    }

    public int size() {
        return _typeList.size();
    }

    public void clear() {
        _typeList = new ArrayList();
        _valueList = new ArrayList();
    }

    public void clear(int i) {
        _typeList.remove(i);
        _valueList.remove(i);
    }

    public Class getType(int i) {
        return(Class)(_typeList.get(i));
    }

    public Object getValue(int i) {
        return _valueList.get(i);
    }

    public Object[] getValueArray() {
        return _valueList.toArray();
    }

    public Object[] getTypeArray() {
        return _typeList.toArray();
    }

    public void startElement(String uri, String localName, String qname, Attributes attr) throws SAXException {
        try {
            if(isJoclNamespace(uri,localName,qname)) {
                if(ELT_OBJECT.equals(localName)) {
                    String cname = getAttributeValue(ATT_CLASS,attr);
                    String isnullstr = getAttributeValue(ATT_ISNULL,attr,"false");
                    boolean isnull = "true".equalsIgnoreCase(isnullstr) || "yes".equalsIgnoreCase(isnullstr);
                    _cur = new ConstructorDetails(cname,_cur,isnull);
                } else if(ELT_ARRAY.equals(localName)) {
                    _cur = new ConstructorDetails(Object[].class,_cur,false,true);
                } else if(ELT_COLLECTION.equals(localName)) {
                    _cur = new ConstructorDetails(Collection.class,_cur,false,true);
                } else if(ELT_LIST.equals(localName)) {
                    _cur = new ConstructorDetails(List.class,_cur,false,true);
                } else if(ELT_BOOLEAN.equals(localName)) {
                    String valstr = getAttributeValue(ATT_VALUE,attr,"false");
                    boolean val = "true".equalsIgnoreCase(valstr) || "yes".equalsIgnoreCase(valstr);
                    addObject(Boolean.TYPE,Boolean.valueOf(val));
                } else if(ELT_BYTE.equals(localName)) {
                    byte val = Byte.parseByte(getAttributeValue(ATT_VALUE,attr,"0"));
                    addObject(Byte.TYPE,new Byte(val));
                } else if(ELT_CHAR.equals(localName)) {
                    char val = '\u0000';
                    String valstr = getAttributeValue(ATT_VALUE,attr);
                    if(null == valstr) {
                        val = '\u0000';
                    } else if(valstr.length() > 1) {
                        throw new SAXException("if present, char value must be exactly one character long");
                    } else if(valstr.length()==1) {
                        val = valstr.charAt(0);
                    } else if(valstr.length()==0) {
                        throw new SAXException("if present, char value must be exactly one character long");
                    }
                    addObject(Character.TYPE,new Character(val));
                } else if(ELT_DOUBLE.equals(localName)) {
                    double val = Double.parseDouble(getAttributeValue(ATT_VALUE,attr,"0"));
                    addObject(Double.TYPE,new Double(val));
                } else if(ELT_FLOAT.equals(localName)) {
                    float val = Float.parseFloat(getAttributeValue(ATT_VALUE,attr,"0"));
                    addObject(Float.TYPE,new Float(val));
                } else if(ELT_INT.equals(localName)) {
                    int val = Integer.parseInt(getAttributeValue(ATT_VALUE,attr,"0"));
                    addObject(Integer.TYPE,new Integer(val));
                } else if(ELT_LONG.equals(localName)) {
                    long val = Long.parseLong(getAttributeValue(ATT_VALUE,attr,"0"));
                    addObject(Long.TYPE,new Long(val));
                } else if(ELT_SHORT.equals(localName)) {
                    short val = Short.parseShort(getAttributeValue(ATT_VALUE,attr,"0"));
                    addObject(Short.TYPE,new Short(val));
                } else if(ELT_STRING.equals(localName)) {
                    String val = getAttributeValue(ATT_VALUE,attr);
                    addObject("".getClass(),val);
                } else {
                    // unrecognized JOCL element warning?
                }
            }
        } catch(NumberFormatException e) {
            throw new SAXException(e);
        } catch(ClassNotFoundException e) {
            throw new SAXException(e);
        }
    }

    public void endElement(String uri, String localName, String qname) throws SAXException {
        try {
            if(isJoclNamespace(uri,localName,qname)) {
                if(ELT_OBJECT.equals(localName) || ELT_ARRAY.equals(localName)
                    || ELT_COLLECTION.equals(localName) || ELT_LIST.equals(localName)) {
                    ConstructorDetails temp = _cur;
                    _cur = _cur.getParent();
                    if(null == _cur) {
                        _typeList.add(temp.getType());
                        _valueList.add(temp.createObject());
                    } else {
                        _cur.addArgument(temp.getType(),temp.createObject());
                    }
                } 
                /* 
                else if(ELT_BOOLEAN.equals(localName)) {
                    // nothing to do here
                } else if(ELT_BYTE.equals(localName)) {
                    // nothing to do here
                } else if(ELT_CHAR.equals(localName)) {
                    // nothing to do here
                } else if(ELT_DOUBLE.equals(localName)) {
                    // nothing to do here
                } else if(ELT_FLOAT.equals(localName)) {
                    // nothing to do here
                } else if(ELT_INT.equals(localName)) {
                    // nothing to do here
                } else if(ELT_LONG.equals(localName)) {
                    // nothing to do here
                } else if(ELT_SHORT.equals(localName)) {
                    // nothing to do here
                } else if(ELT_STRING.equals(localName)) {
                    // nothing to do here
                } else {
                    // unrecognized JOCL element warning?
                }
                */
            }
        } catch(Exception e) {
            throw new SAXException(e);
        }
    }

    public void setDocumentLocator(Locator locator) {
        _locator = locator;
    }

    protected boolean isJoclNamespace(String uri, String localname, String qname) {
        if(JOCL_NAMESPACE_URI.equals(uri)) {
            return true;
        } else if(_acceptEmptyNamespaceForElements && (null == uri || "".equals(uri))) {
            return true;
        } else if(_acceptJoclPrefixForElements && (null == uri || "".equals(uri)) && qname.startsWith(JOCL_PREFIX)) {
            return true;
        } else {
            return false;
        }
    }

    protected String getAttributeValue(String localname, Attributes attr) {
        return getAttributeValue(localname,attr,null);
    }

    protected String getAttributeValue(String localname, Attributes attr, String implied) {
        String val = attr.getValue(JOCL_NAMESPACE_URI,localname);
        if(null == val && _acceptEmptyNamespaceForAttributes) {
            val = attr.getValue("",localname);
        }
        if(null == val && _acceptJoclPrefixForAttributes) {
            val = attr.getValue("",JOCL_PREFIX + localname);
        }
        return null == val ? implied : val;
    }

    protected void addObject(Class type, Object val) {
        if(null == _cur) {
            _typeList.add(type);
            _valueList.add(val);
        } else {
            _cur.addArgument(type,val);
        }
    }

    public static final String JOCL_NAMESPACE_URI = "http://apache.org/xml/xmlns/jakarta/commons/jocl";

    public static final String JOCL_PREFIX = "jocl:";

    protected ArrayList _typeList = new ArrayList();

    protected ArrayList _valueList = new ArrayList();

    protected ConstructorDetails _cur = null;

    protected boolean _acceptEmptyNamespaceForElements = true;

    protected boolean _acceptJoclPrefixForElements = true;

    protected boolean _acceptEmptyNamespaceForAttributes = true;

    protected boolean _acceptJoclPrefixForAttributes = true;

    protected Locator _locator = null;

    protected static final String ELT_OBJECT  = "object";

    protected static final String ELT_ARRAY  = "array";

    protected static final String ELT_COLLECTION  = "collection";

    protected static final String ELT_LIST = "list";

    protected static final String ATT_CLASS   = "class";

    protected static final String ATT_ISNULL  = "null";

    protected static final String ELT_BOOLEAN = "boolean";

    protected static final String ELT_BYTE    = "byte";

    protected static final String ELT_CHAR    = "char";

    protected static final String ELT_DOUBLE  = "double";

    protected static final String ELT_FLOAT   = "float";

    protected static final String ELT_INT     = "int";

    protected static final String ELT_LONG    = "long";

    protected static final String ELT_SHORT   = "short";

    protected static final String ELT_STRING  = "string";

    protected static final String ATT_VALUE   = "value";

    static class ConstructorDetails {
        private ConstructorDetails _parent = null;
        private Class _type = null;
        private ArrayList _argTypes = null;
        private ArrayList _argValues = null;
        private boolean _isnull = false;
        private boolean _isgroup = false;

        public ConstructorDetails(String classname, ConstructorDetails parent) throws ClassNotFoundException {
            this(Class.forName(classname),parent,false,false);
        }

        public ConstructorDetails(String classname, ConstructorDetails parent, boolean isnull) throws ClassNotFoundException {
            this(Class.forName(classname),parent,isnull,false);
        }

        /**
         * @since 1.3
         */
        public ConstructorDetails(String classname, ConstructorDetails parent, boolean isnull, boolean isgroup) throws ClassNotFoundException {
            this(Class.forName(classname),parent,isnull,isgroup);
        }

        /**
         * @since 1.3
         */
        public ConstructorDetails(Class type, ConstructorDetails parent, boolean isnull, boolean isgroup) {
            _parent = parent;
            _type = type;
            _argTypes = new ArrayList();
            _argValues = new ArrayList();
            _isnull = isnull;
            _isgroup = isgroup;
        }

        public void addArgument(Object value) {
            addArgument(value.getClass(),value);
        }

        public void addArgument(Class type, Object val) {
            if(_isnull) {
                throw new NullPointerException("can't add arguments to null instances");
            }
            _argTypes.add(type);
            _argValues.add(val);
        }

        public Class getType() {
            return _type;
        }

        public ConstructorDetails getParent() {
            return _parent;
        }

        public Object createObject() throws InstantiationException, IllegalAccessException, InvocationTargetException {
            if(_isnull) {
                return null;
            } else if( _isgroup ) {
                if (_type.equals(Object[].class)) {
                    return _argValues.toArray();
                } else if (_type.equals(Collection.class) || _type.equals(List.class)) {
                    return _argValues;
                } else {
                    throw new IllegalStateException("implementation error: unhandled _type:" + _type);
                }
            } else {
                Class k = getType();
                Class[] argtypes = (Class[])_argTypes.toArray(new Class[0]);
                Object[] argvals = _argValues.toArray();
                return ConstructorUtil.invokeConstructor(k,argtypes,argvals);
            }
        }
    }

}
