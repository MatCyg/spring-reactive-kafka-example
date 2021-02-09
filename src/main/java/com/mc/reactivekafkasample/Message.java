package com.mc.reactivekafkasample;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class Message {

    private final String id;
    private final String msg;
}
