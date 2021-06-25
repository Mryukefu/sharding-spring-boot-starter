package com.dc.game.shardingspringbootstarter.registrar;
import com.dc.game.shardingspringbootstarter.annotation.MyMapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import tk.mybatis.spring.mapper.ClassPathMapperScanner;
import tk.mybatis.spring.mapper.MapperFactoryBean;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;

public class MyMapperScannerRegistrar implements ImportBeanDefinitionRegistrar, ResourceLoaderAware, EnvironmentAware {

    private  static  final Logger LOGGER = LoggerFactory.getLogger(MyMapperScannerRegistrar. class);

    private Environment environment;
 
    private ResourceLoader resourceLoader;
 

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
 
    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }
 
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        AnnotationAttributes mapperScanAttrs = AnnotationAttributes.fromMap(importingClassMetadata.getAnnotationAttributes(MyMapperScan.class.getName()));
        if (mapperScanAttrs != null) {
            this.registerBeanDefinitions(mapperScanAttrs, registry);
        }
    }
 
    void registerBeanDefinitions(AnnotationAttributes annoAttrs, BeanDefinitionRegistry registry) {
        ClassPathMapperScanner scanner = new ClassPathMapperScanner(registry);
        Optional var10000 = Optional.ofNullable(this.resourceLoader);
        Objects.requireNonNull(scanner);
        Class<? extends Annotation> annotationClass = annoAttrs.getClass("annotationClass");
        if (!Annotation.class.equals(annotationClass)) {
            scanner.setAnnotationClass(annotationClass);
        }
 
        Class<?> markerInterface = annoAttrs.getClass("markerInterface");
        if (!Class.class.equals(markerInterface)) {
            scanner.setMarkerInterface(markerInterface);
        }
 
        Class<? extends BeanNameGenerator> generatorClass = annoAttrs.getClass("nameGenerator");
        if (!BeanNameGenerator.class.equals(generatorClass)) {
            scanner.setBeanNameGenerator(BeanUtils.instantiateClass(generatorClass));
        }
 
        Class<? extends MapperFactoryBean> mapperFactoryBeanClass = annoAttrs.getClass("factoryBean");
        if (!MapperFactoryBean.class.equals(mapperFactoryBeanClass)) {
            scanner.setMapperFactoryBean(BeanUtils.instantiateClass(mapperFactoryBeanClass));
        }
 
        scanner.setSqlSessionTemplateBeanName(annoAttrs.getString("sqlSessionTemplateRef"));
        scanner.setSqlSessionFactoryBeanName(annoAttrs.getString("sqlSessionFactoryRef"));
        List<String> basePackages = new ArrayList<String>();
        basePackages.addAll(Arrays.stream(annoAttrs.getStringArray("value")).filter(StringUtils::hasText).collect(Collectors.toList()));
        for (String pkg : annoAttrs.getStringArray("basePackages")) {
            if (StringUtils.hasText(pkg)) {
                String value = parsePlaceHolder(pkg);
                if(StringUtils.hasText(value)){
                    List<String> valueList = Arrays.asList(value.split(","));
                    for(String base : valueList){
                        basePackages.add(base);
                    }
                }
            }
        }
        basePackages.addAll(Arrays.stream(annoAttrs.getClassArray("basePackageClasses"))
                .map(ClassUtils::getPackageName).collect(Collectors.toList()));

        String mapperHelperRef = annoAttrs.getString("mapperHelperRef");
        String[] properties = annoAttrs.getStringArray("properties");
        if (StringUtils.hasText(mapperHelperRef)) {
            scanner.setMapperHelperBeanName(mapperHelperRef);
        } else if (properties != null && properties.length > 0) {
            scanner.setMapperProperties(properties);
        } else {
            try {
                scanner.setMapperProperties(this.environment);
            } catch (Exception var14) {
                LOGGER.warn("只有 Spring Boot 环境中可以通过 Environment(配置文件,环境变量,运行参数等方式) 配置通用 Mapper，其他环境请通过 @MapperScan 注解中的 mapperHelperRef 或 properties 参数进行配置!如果你使用 tk.mybatis.mapper.session.Configuration 配置的通用 Mapper，你可以忽略该错误!", var14);
            }
        }

        scanner.registerFilters();
        scanner.doScan(StringUtils.toStringArray(basePackages));
    }
 
    private String parsePlaceHolder(String pro) {
        if (StringUtils.hasText(pro) && pro.contains(PropertySourcesPlaceholderConfigurer.DEFAULT_PLACEHOLDER_PREFIX)) {
            String value = environment.getProperty(pro.substring(2, pro.length() - 1));
 
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("find placeholder value " + value + " for key " + pro);
            }
            if (null == value) {
                throw new IllegalArgumentException("property " + pro + " can not find!!!");
            }
            return value;
        }
        return pro;
    }
}
