package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.StatusManager;
import ch.qos.logback.core.status.WarnStatus;
import org.slf4j.Logger;
import org.slf4j.Marker;

public class BlindLogger implements Logger {

    private final String name;
    private final StatusManager statusManager;

    public BlindLogger(String name, StatusManager statusManager) {
        this.name = name;
        this.statusManager = statusManager;
    }

    private void warnStatus(String msg, Throwable e) {
        statusManager.add(new WarnStatus(msg, null, e));
    }

    private void erorStatus(String msg, Throwable e) {
        statusManager.add(new ErrorStatus(msg, null, e));
    }

    private void infoStatus() {
        boolean noop = true;
    }


    private String formatString(String format, Object... args) {
        for (Object arg : args) {
            format = format.replaceFirst("\\{\\}", arg.toString());
        }
        return format;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public void warn(String msg) {
        warnStatus(msg, null);
    }

    @Override
    public void warn(String format, Object arg) {
        warnStatus(formatString(format, arg), null);
    }

    @Override
    public void warn(String format, Object... arguments) {
        warnStatus(format, null);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        warnStatus(format, null);
    }

    @Override
    public void warn(String msg, Throwable t) {
        warnStatus(msg,t);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return true;
    }

    @Override
    public void warn(Marker marker, String msg) {
        warnStatus(msg, null);
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        warnStatus(formatString(format, arg), null);
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        warnStatus(formatString(format, arg1, arg2), null);
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        warnStatus(formatString(format, arguments), null);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        warnStatus(msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public void error(String msg) {
        erorStatus(msg, null);
    }

    @Override
    public void error(String format, Object arg) {
        erorStatus(formatString(format, arg), null);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        erorStatus(formatString(format, arg1, arg2), null);
    }

    @Override
    public void error(String format, Object... arguments) {
        erorStatus(formatString(format, arguments), null);
    }

    @Override
    public void error(String msg, Throwable t) {
        erorStatus(msg, t);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return true;
    }

    @Override
    public void error(Marker marker, String msg) {
        erorStatus(msg, null);
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        erorStatus(formatString(format, arg), null);
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        erorStatus(formatString(format, arg1, arg2), null);
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        erorStatus(formatString(format, arguments), null);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        erorStatus(msg, t);
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public void trace(String msg) {  }

    @Override
    public void trace(String format, Object arg) { }

    @Override
    public void trace(String format, Object arg1, Object arg2) {  }

    @Override
    public void trace(String format, Object... arguments) {  }

    @Override
    public void trace(String msg, Throwable t) { }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return false;
    }

    @Override
    public void trace(Marker marker, String msg) {

    }

    @Override
    public void trace(Marker marker, String format, Object arg) {

    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {

    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {

    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {

    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void debug(String msg) {

    }

    @Override
    public void debug(String format, Object arg) {

    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {

    }

    @Override
    public void debug(String format, Object... arguments) {

    }

    @Override
    public void debug(String msg, Throwable t) {

    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return false;
    }

    @Override
    public void debug(Marker marker, String msg) {

    }

    @Override
    public void debug(Marker marker, String format, Object arg) {

    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {

    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {

    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {

    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public void info(String msg) {
        infoStatus();
    }

    @Override
    public void info(String format, Object arg) {
        infoStatus();
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        infoStatus();
    }

    @Override
    public void info(String format, Object... arguments) {
        infoStatus();
    }

    @Override
    public void info(String msg, Throwable t) {
        infoStatus();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return false;
    }

    @Override
    public void info(Marker marker, String msg) {
        infoStatus();
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        infoStatus();
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        infoStatus();
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        infoStatus();
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        infoStatus();
    }


}
