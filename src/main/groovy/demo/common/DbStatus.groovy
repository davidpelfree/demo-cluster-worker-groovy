package demo.common

/**
 * Values for 'status' column in 'processing' DB table.
 */
enum DbStatus {

    // Using lower case strings.
    // They are used inside DB so don't change without checking DB existing values first.

    fetched,
    processing,
    completed,

    failed,
    failed_upload_to_s3,
    failed_download_from_s3,
}
