package com.hyeonju.outbox.common.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * description    : 공통 API 응답 (code, desc, status, data)
 * fileName       : ResponseBaseDto
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Getter
@AllArgsConstructor
public class ResponseBaseDto<T> {

    private final String code;
    private final String desc;
    private final int status;
    private final T data;

    public static <T> ResponseBaseDto<T> ok(T data) {
        return new ResponseBaseDto<>("SUCCESS", "성공", 200, data);
    }
}
