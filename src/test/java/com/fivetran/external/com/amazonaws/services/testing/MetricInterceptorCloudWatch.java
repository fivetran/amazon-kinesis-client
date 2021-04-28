package com.fivetran.external.com.amazonaws.services.testing;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;
import com.amazonaws.services.cloudwatch.waiters.AmazonCloudWatchWaiters;

/**
 * The DynamoDB Kinesis Streams Adapter wants to maintain a CloudWatch metric for each table. We don't need that, so we
 * log the metrics instead.
 *
 * <p>Amazon advises against implementing this interface yourself because the methods frequently change. Consequently,
 * this class will likely need work if we ever upgrade our AWS libraries. {@see
 * com.amazonaws.services.kinesis.metrics.impl.DefaultCWMetricsPublisher#cloudWatchClient} to determine the appropriate
 * responses to stub.
 */
public class MetricInterceptorCloudWatch implements AmazonCloudWatch {

    @Override
    public void setEndpoint(String endpoint) {}

    @Override
    public void setRegion(Region region) {}

    @Override
    public DeleteAlarmsResult deleteAlarms(DeleteAlarmsRequest deleteAlarmsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteDashboardsResult deleteDashboards(DeleteDashboardsRequest deleteDashboardsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeAlarmHistoryResult describeAlarmHistory(DescribeAlarmHistoryRequest describeAlarmHistoryRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeAlarmHistoryResult describeAlarmHistory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeAlarmsResult describeAlarms(DescribeAlarmsRequest describeAlarmsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeAlarmsResult describeAlarms() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeAlarmsForMetricResult describeAlarmsForMetric(
            DescribeAlarmsForMetricRequest describeAlarmsForMetricRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DisableAlarmActionsResult disableAlarmActions(DisableAlarmActionsRequest disableAlarmActionsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EnableAlarmActionsResult enableAlarmActions(EnableAlarmActionsRequest enableAlarmActionsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetDashboardResult getDashboard(GetDashboardRequest getDashboardRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetMetricDataResult getMetricData(GetMetricDataRequest getMetricDataRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetMetricStatisticsResult getMetricStatistics(GetMetricStatisticsRequest getMetricStatisticsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetMetricWidgetImageResult getMetricWidgetImage(GetMetricWidgetImageRequest getMetricWidgetImageRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListDashboardsResult listDashboards(ListDashboardsRequest listDashboardsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListMetricsResult listMetrics(ListMetricsRequest listMetricsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListMetricsResult listMetrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutDashboardResult putDashboard(PutDashboardRequest putDashboardRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutMetricAlarmResult putMetricAlarm(PutMetricAlarmRequest putMetricAlarmRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutMetricDataResult putMetricData(PutMetricDataRequest putMetricDataRequest) {
        System.out.println("Metric data: " + putMetricDataRequest.getMetricData());

        // Return null so that a NullPointerException will be thrown if value is ever used
        return null;
    }

    @Override
    public SetAlarmStateResult setAlarmState(SetAlarmStateRequest setAlarmStateRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {}

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AmazonCloudWatchWaiters waiters() {
        throw new UnsupportedOperationException();
    }
}