package com.ery.ertc.collect.ui;

import org.mortbay.jetty.Server;

public abstract class Handler implements org.mortbay.jetty.Handler{

    @Override
    public Server getServer() {
        return null;
    }

    @Override
    public void setServer(Server server) {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public boolean isStarting() {
        return false;
    }

    @Override
    public boolean isStopping() {
        return false;
    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public boolean isFailed() {
        return false;
    }

    @Override
    public void addLifeCycleListener(Listener listener) {
    }

    @Override
    public void removeLifeCycleListener(Listener listener) {
    }
}
