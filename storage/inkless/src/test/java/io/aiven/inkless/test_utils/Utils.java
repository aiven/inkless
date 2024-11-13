// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import java.util.concurrent.Executor;

public class Utils {
    public static final Executor SYNC_EXECUTOR = Runnable::run;
}
