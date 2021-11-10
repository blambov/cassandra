/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.schema;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.SkipListMemtableFactory;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Memtable types and options are specified with these parameters. Memtable classes must either contain a static FACTORY
 * field (if they take no arguments other than class), or implement a factory(Map<String, String>) method.
 *
 * The latter should consume any further options (using map.remove).
 *
 *
 * CQL: {'class' : 'SkipListMemtable'}
 */
public final class MemtableParams
{
    public final Memtable.Factory factory;
    public final ImmutableMap<String, String> options;

    private MemtableParams(Memtable.Factory factory, ImmutableMap<String, String> options)
    {
        this.options = options;
        this.factory = factory;
    }

    private static Memtable.Factory getMemtableFactory(Map<String, String> options)
    {
        Map<String, String> copy = new HashMap<>(options);
        String className = copy.remove(Option.CLASS.toString());
        if (className.isEmpty() || className == null)
            throw new ConfigurationException(
            "The 'class' option must not be empty. To use default implementation, remove option.");

        className = className.contains(".") ? className : "org.apache.cassandra.db.memtable." + className;
        try
        {
            Memtable.Factory factory;
            Class<?> clazz = Class.forName(className);
            try
            {
                Method factoryMethod = clazz.getDeclaredMethod("factory", Map.class);
                factory = (Memtable.Factory) factoryMethod.invoke(null, copy);
            }
            catch (NoSuchMethodException e)
            {
                // continue with FACTORY field
                Field factoryField = clazz.getDeclaredField("FACTORY");
                factory = (Memtable.Factory) factoryField.get(null);
            }
            if (!copy.isEmpty())
                throw new ConfigurationException("Memtable class " + className + " does not accept any futher parameters, but " +
                                                 copy + " were given.");
            return factory;
        }
        catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | ClassCastException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException) e.getCause();
            throw new ConfigurationException("Could not create memtable factory for type " + className +
                                             " and options " + copy, e);
        }
    }

    public static MemtableParams fromMap(Map<String, String> map)
    {
        if (map == null || map.isEmpty())
            return DEFAULT;

        MemtableParams byTemplate = getTemplate(map);
        if (byTemplate != null)
            return byTemplate;

        return new MemtableParams(getMemtableFactory(map), ImmutableMap.copyOf(map));
    }

    private static MemtableParams getTemplate(Map<String, String> map)
    {
        String template = map.get(TEMPLATE_OPTION);
        if (template == null)
            return null;

        if (map.size() != 1)
            throw new ConfigurationException("When a memtable template is specified no other parameters can be given, was " + map);
        MemtableParams params = templates.get(template);
        if (params == null)
            throw new ConfigurationException("Memtable template " + template + " not found.");
        return params;
    }

    public Map<String, String> asMap()
    {
        // options is an immutable map, ok to share
        return options;
    }

    @Override
    public String toString()
    {
        return options.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof MemtableParams))
            return false;

        MemtableParams c = (MemtableParams) o;

        return Objects.equal(options, c.options);
    }

    @Override
    public int hashCode()
    {
        return factory.hashCode();
    }

    public enum Option
    {
        CLASS;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public static final String TEMPLATE_OPTION = "template";
    public static final Map<String, MemtableParams> templates;
    public static final MemtableParams DEFAULT;
    static {
        templates = parseTemplates(DatabaseDescriptor.getMemtableTemplates());
        DEFAULT = new MemtableParams(getDefaultFactory(DatabaseDescriptor.getMemtableDefault()), ImmutableMap.of());
    }

    private static Map<String, MemtableParams> parseTemplates(Map<String, Map<String, Object>> templateDefinition)
    {
        if (templateDefinition == null)
            return ImmutableMap.of();

        Map<String, MemtableParams> templates = new HashMap<>(templateDefinition.size());
        for (Map.Entry<String, Map<String, Object>> definition : templateDefinition.entrySet())
        {
            String template = definition.getKey();
            templates.put(template, new MemtableParams(getMemtableFactory(toStringMap(definition.getValue())),
                                                       ImmutableMap.of(TEMPLATE_OPTION, template)));
        }
        return templates;
    }

    private static Map<String, String> toStringMap(Map<String, Object> objectMap)
    {
        return Maps.transformValues(objectMap, Object::toString);
    }

    private static Memtable.Factory getDefaultFactory(Map<String, String> defaultOptions)
    {
        if (defaultOptions == null || defaultOptions.isEmpty())
            return SkipListMemtableFactory.INSTANCE;
        MemtableParams byTemplate = getTemplate(defaultOptions);
        if (byTemplate != null)
            return byTemplate.factory;
        else
            return getMemtableFactory(defaultOptions);
    }
}
