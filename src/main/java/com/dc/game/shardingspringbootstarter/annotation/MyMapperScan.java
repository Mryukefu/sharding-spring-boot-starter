//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.dc.game.shardingspringbootstarter.annotation;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.dc.game.shardingspringbootstarter.registrar.MyMapperScannerRegistrar;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.annotation.Import;
import tk.mybatis.spring.mapper.MapperFactoryBean;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
@Import({MyMapperScannerRegistrar.class})
public @interface MyMapperScan {
    String[] value() default {};

    String[] basePackages() default {};

    Class<?>[] basePackageClasses() default {};

    Class<? extends BeanNameGenerator> nameGenerator() default BeanNameGenerator.class;

    Class<? extends Annotation> annotationClass() default Annotation.class;

    Class<?> markerInterface() default Class.class;

    String sqlSessionTemplateRef() default "";

    String sqlSessionFactoryRef() default "";

    Class<? extends MapperFactoryBean> factoryBean() default MapperFactoryBean.class;

    String[] properties() default {};

    String mapperHelperRef() default "";
}
