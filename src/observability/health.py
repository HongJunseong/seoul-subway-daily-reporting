"""Turn measured data issues into recovery guidance without inferring API failures."""

GUIDANCE = {
    'arrival_unprocessed_snapshots': ('processing', '저장된 Bronze 구간을 Silver부터 재처리한다. API 재수집은 필요하지 않다.'),
    'arrival_unobserved_time_buckets': ('collection', '수집 태스크와 원본 기록을 확인한다. 관측 부재만으로 API 실패를 단정하거나 과거 실시간 값을 복원하지 않는다.'),
    'arrival_stale': ('freshness', '조회 구간과 감사 기준 시각을 먼저 확인한다. 과거 구간의 관측 나이를 현재 파이프라인 장애로 단정하지 않는다.'),
    'arrival_response_at_limit': ('coverage', '응답 상한에 도달한 표본을 부분 관측으로 표시한다. 전체 네트워크 커버리지로 해석하지 않는다.'),
    'arrival_source_clock_ahead': ('source_time', '원천 시각과 수집 시각을 비교한다. 값을 임의 보정하지 않고 원인을 확인한다.'),
    'usage_silver_gold_mismatch': ('processing', '동시 writer를 중지하고 완료된 실행인지 확인한 뒤 저장된 Bronze로 Silver·Gold를 재처리한다.'),
    'arrival_window_empty': ('collection', '해당 구간 Bronze와 처리 기록을 확인한다. 빈 관측을 정상 0건으로 출판하지 않는다.'),
}


def pipeline_health(quality, tasks=None, campaign=None):
    diagnostics = []
    for issue in quality['issues']:
        code = issue['code']
        if code.startswith('source_'):
            category, action = 'storage', 'Delta 루트·날짜·스키마와 접근 권한을 확인한다. 내부 Parquet를 직접 읽지 않는다.'
        else:
            category, action = GUIDANCE.get(code, ('quality', '원본의 키·날짜·수치와 최신 정정을 확인하고 품질 오류 해결 후 재처리한다.'))
        diagnostics.append(dict(issue, category=category, action=action))
    if tasks is not None:
        if tasks['malformed_events'] or not tasks['unique_events']:
            diagnostics.append({'severity': 'warning', 'category': 'task_logs', 'code': 'task_logs_unusable',
                                'action': '로그 수집·잘림·이벤트 형식을 확인하고 Airflow의 최종 상태를 조회한다.'})
        for field in ('failed', 'incomplete', 'conflicting'):
            count = sum(task[field] for task in tasks['tasks'])
            if count:
                diagnostics.append({'severity': 'warning', 'category': 'task_logs', 'code': 'task_attempts_' + field,
                                    'count': count, 'action': '해당 run·시도의 Airflow 최종 상태와 후속 재시도를 확인한다. 과거 실패 시도를 현재 장애로 단정하지 않는다.'})
    if campaign is not None:
        counts = campaign['totals']
        for key in ('unconfirmed_due_slots', 'failed_request_slots', 'missed_start_window_slots', 'missed_capture_schedule_slots'):
            if counts[key]:
                diagnostics.append({'severity': 'warning', 'category': 'campaign', 'code': key, 'count': counts[key],
                                    'action': '고정 계획과 실제 예약·캡처 기록을 대조한다. 누락 기록을 호출 0회로 간주하거나 예약을 삭제해 재수집하지 않는다.'})
    status = 'failed' if any(d['severity'] == 'error' for d in diagnostics) else ('degraded' if diagnostics else 'healthy')
    return {'schema_version': 1, 'status': status, 'data_kind': quality['data_kind'],
            'quality': quality, 'tasks': tasks, 'campaign': campaign, 'diagnostics': diagnostics,
            'limitations': ['Read-only data/log check; not authoritative scheduler health',
                            'No automatic retries, API requests, notifications or repair',
                            'Absent task logs mean not checked, not task success']}


def render_health(health):
    lines = ['# 파이프라인 점검', '', f"상태: {health['status']} · 입력: {health['data_kind']}",
             f"점검 기준(KST): {health['quality']['window']['as_of_kst']}", '',
             '태스크 로그: ' + ('제공됨; Airflow 최종 상태는 별도 확인' if health['tasks'] is not None else '미제공·미검증'), '',
             '| 구분 | 판정 | 다음 조치 |', '|---|---|---|']
    lines += [f"| {item['category']} | {item['code']} | {item['action']} |" for item in health['diagnostics']]
    lines += ['', 'API 호출·알림·자동 복구는 수행하지 않는다. 전체 네트워크 커버리지나 scheduler 정상 여부를 증명하지 않는다.', '']
    return '\n'.join(lines)
