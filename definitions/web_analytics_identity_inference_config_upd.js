module.exports = {

    afqts: {
        startDate: "2026-03-01",

        identity: {
            anchorSources: [{
                    entityTableName: "teachers",
                    requestPaths: [
                        "/teacher",
                        "/teacher/auth/gov_one/callback",
                        "/teacher/magic_link"
                    ],
                    requestMethods: ["GET", "POST", "HEAD"],
                    dataField: "data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "application_forms",
                    requestPaths: [
                        "/teacher/*"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "data",
                    userIdKey: "teacher_id"
                },
                {
                    entityTableName: "application_forms",
                    requestPaths: [
                        "/assessor/*"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "assessor_id"
                },
                {
                    entityTableName: "staff",
                    requestPaths: [
                        "/staff/auth/entra_id/callback",
                        "/staff/sign_in",
                        "/assessor/applications"
                    ],
                    requestMethods: ["POST", "GET"],
                    dataField: "data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "notes",
                    requestPaths: [
                        "/assessor/applications/*"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "author_id"
                }
            ]
        },

        paths: {
            preAuth: [
                "/",
                "/?",
                "/eligibility/start",
                "/teacher/sign_in_or_sign_up",
                "/eligibility/qualifications",
                "/eligibility/degree",
                "/eligibility/work-experience",
                "/eligibility/work-experience-referee",
                "/eligibility/misconduct",
                "/eligibility/work-experience-in-england",
                "/eligibility/teach-children",
                "/eligibility/result"
            ],

            signIn: [
                "/teacher/magic_link",
                "/teacher/sign_in",
                "/staff/sign_in",
                "/staff/auth/entra_id/callback"
            ],

            signOut: [
                "/teacher/signed_out",
                "/teacher/sign_out",
                "/staff/sign_out",
                "/staff/signed_out"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/support/.*",
                ".*/staff/.*",
                ".*/assessor/.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    },

    apply: {
        startDate: "2025-06-01",

        identity: {
            anchorSources: [{
                    entityTableName: "candidates",
                    requestPaths: [
                        "/candidate/interstitial",
                        "/candidate/apply",
                        "/candidate/previous-teacher-training/complete",
                        "/candidate/previous-teacher-training/*/publish",
                        "/candidate/dismiss-account-recovery/create",
                        "/candidate/previous-teacher-training/*",
                        "/candidate/application/*",
                        "/api/*"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "hidden_data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "application_forms",
                    requestPaths: [
                        "/candidate/previous-teacher-training/complete",
                        "/candidate/previous-teacher-training/*/publish",
                        "/candidate/sign-in/confirm",
                        "/candidate/previous-teacher-training/*",
                        "/candidate/interstitial"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "hidden_data",
                    userIdKey: "candidate_id"
                },
                {
                    entityTableName: "pool_invites",
                    requestPaths: [
                        "/provider/find-candidates/*/invite",
                        "/provider/find-candidates/*/invite/*/message",
                        "/provider/find-candidates/*/invite/*/review",
                        "/provider/find-candidates/*/invite/*"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "invited_by_id"
                },
                {
                    entityTableName: "notes",
                    requestPaths: [
                        "/provider/applications/*/notes"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "user_id"
                },
                {
                    entityTableName: "validation_errors",
                    requestPaths: [
                        "/provider/applications/*/interviews/check",
                        "/provider/applications/*/offer/conditions",
                        "/provider/applications/*/rejections",
                        "/provider/applications/*/decision",
                        "/provider/applications/*/offer/locations",
                        "/provider/applications/*/interviews/*/check",
                        "/provider/applications/*/courses/locations",
                        "/provider/organisation-settings/organisations/*/user",
                        "/provider/applications/*/notes",
                        "/provider/applications/*/interviews/*/cancel",
                        "/provider/applications/*/interviews",
                        "/provider/applications/*/offer/ske-requirements"
                    ],
                    requestMethods: ["POST"],
                    dataField: "hidden_data",
                    userIdKey: "user_id"
                },
                {
                    entityTableName: "account_recovery_requests",
                    requestPaths: [
                        "/candidate/account-recovery-requests"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "candidate_id"
                },
                {
                    entityTableName: "provider_user_notifications",
                    requestPaths: [
                        "/provider/account/notification-settings"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "provider_user_id"
                },
                {
                    entityTableName: "one_login_auths",
                    requestPaths: [
                        "/candidate/account-recovery/create"
                    ],
                    requestMethods: ["POST"],
                    dataField: "hidden_data",
                    userIdKey: "candidate_id"
                },
                {
                    entityTableName: "provider_agreements",
                    requestPaths: [
                        "/provider/data-sharing-agreements"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "provider_user_id"
                }
            ]
        },

        paths: {
            preAuth: [
                "/",
                "/?"
            ],

            signIn: [
                "/candidate/sign-in/check-email",
                "/candidate/sign-in/confirm",
                "/provider/sign-in",
                "/candidate/sign-in",
                "/support/sign-in",
                "/login"
            ],

            signOut: [
                "/candidate/sign-out",
                "/candidate/sign-in/expired",
                "/candidate/sign-out",
                "/auth/dfe-support/sign-out"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/support/.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    },

    gse: {
        startDate: "2026-01-01",

        identity: {
            anchorSources: [{
                entityTableName: "users",
                requestPaths: [
                    "/auth/callback"
                ],
                requestMethods: ["GET"],
                dataField: "hidden_data",
                userIdKey: "sub"
            }]
        },

        paths: {
            preAuth: [
                "/",
                "/?"
            ],

            signIn: [
                "/candidate/sign-in/check-email",
                "/candidate/sign-in/confirm",
                "/provider/sign-in",
                "/candidate/sign-in",
                "/support/sign-in",
                "/login"
            ],

            signOut: [
                "/candidate/sign-out",
                "/candidate/sign-in/expired",
                "/candidate/sign-out",
                "/auth/dfe-support/sign-out"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/support/.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    },

    publish: {
        startDate: "2026-03-01",

        identity: {
            anchorSources: [{
                    entityTableName: "saved_course",
                    requestPaths: [
                        "/candidate/saved-courses",
                        "/candidate/saved-courses/*",
                        "/candidate/saved-courses/after_auth",
                        "/candidate/saved-courses/*/note",
                        "/candidate/saved-courses/undo"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "candidate_id"
                },
                {
                    entityTableName: "saved_course",
                    requestPaths: [
                        "/candidate/saved-courses/*"
                    ],
                    requestMethods: ["DELETE"],
                    dataField: "data",
                    userIdKey: "candidate_id"
                },
                {
                    entityTableName: "saved_course",
                    requestPaths: [
                        "/candidate/saved-courses/after_auth"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "candidate_id"
                },
                {
                    entityTableName: "session",
                    requestPaths: [
                        "/auth/one-login/callback",
                        "/results",
                        "/course/*",
                        "/geolocation-suggestions",
                        "/candidate/saved-courses",
                        "/candidate/saved-courses/after_auth",
                        "/candidate/saved-courses/*",
                        "/",
                        "/course/*/confirm-apply",
                        "/course/*/apply",
                        "/course/*/placements",
                        "/publish/organisations",
                        "/publish/organisations/*",
                        "/publish/organisations*",
                        "/primary",
                        "/secondary",
                        "/support/feedback",
                        "/candidate/recent-searches",
                        "/course/*/provider/website",
                        "/favicon.ico",
                        "/support/*/providers",
                        "/auth/dfe/signout",
                        "/candidate/saved-courses/*/note/edit",
                        "/assets/publish/@ministryofjustice/frontend/moj/assets/images/icon-tag-remove-cross.svg",
                        "/sign-out",
                        "/publish/providers/suggest",
                        "/support/*/providers/*/courses",
                        "/support/*/providers/*",
                        "/support",
                        "/support/feedback/delete_multiple",
                        "/publish/providers/search",
                        "/candidate/saved-courses/sign_in",
                        "/support/*/users",
                        "/sign-in",
                        "/support/*/providers/*/users",
                        "/assets/publish/@ministryofjustice/frontend/moj/assets/images/icon-tag-remove-cross-white.svg",
                        "/support/providers-onboarding-form-requests",
                        "/apple-touch-icon.png",
                        "/apple-touch-icon-precomposed.png",
                        "/support/*/providers/*/courses/*/edit",
                        "/support/*/users/*",
                        "/support/providers-onboarding-form-requests/*",
                        "/support/*/providers/*/schools",
                        "/course/*/git/teacher_training_advisers",
                        "/support/*/users/*/providers",
                        "/feedback/new",
                        "/publish/notifications",
                        "/course/*/",
                        "/candidate/recent-searches/clear_all",
                        "/support/*/providers/*/users/*",
                        "/support/providers-onboarding-form-requests/new"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "sessionable_id"
                },
                {
                    entityTableName: "session",
                    requestPaths: [
                        "/candidate/saved-courses",
                        "/candidate/saved-courses/*",
                        "/sign-out",
                        "/primary",
                        "/secondary",
                        "/support/feedback/delete_multiple",
                        "/candidate/saved-courses/undo",
                        "/publish/accept-terms",
                        "/candidate/recent-searches/clear_all"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "sessionable_id"
                },
                {
                    entityTableName: "candidate",
                    requestPaths: [
                        "/auth/one-login/callback"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "authentication",
                    requestPaths: [
                        "/auth/one-login/callback"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "authenticable_id"
                },
                {
                    entityTableName: "user",
                    requestPaths: [
                        "/auth/dfe/callback"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "user",
                    requestPaths: [
                        "/publish/accept-terms"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "id"
                }
            ]
        },

        paths: {
            preAuth: [
                "/",
                "/?"
            ],

            signIn: [
                "/auth/one-login/callback",
                "/candidate/saved-courses/sign_in",
                "/candidate/saved-courses/after_auth",
                "/sign-in",
                "/auth/dfe/callback"
            ],

            signOut: [
                "/sign-out",
                "/auth/dfe/signout",
                "/auth/one-login/backchannel-logout"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/support.*",
                ".*/wp-admin.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    },

    register: {
        startDate: "2026-01-01",

        identity: {
            anchorSources: [{
                    entityTableName: "users",
                    requestPaths: [
                        "/auth/dfe/callback"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "activities",
                    requestPaths: [
                        "/trainees"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "user_id"
                }
            ]
        },

        paths: {
            preAuth: [
                "/",
                "/?"
            ],

            signIn: [
                "/auth/dfe/callback",
                "/sign-in/",
                "/sign-in"
            ],

            signOut: [
                "/sign-out",
                "/auth/dfe/sign-out",
                "/auth/failure"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/system-admin.*",
                ".*/wp-admin.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    },

    itt_mentor: {
        startDate: "2025-06-01",

        identity: {
            anchorSources: [{
                    entityTableName: "users",
                    requestPaths: [
                        "/auth/dfe/callback"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "claims",
                    requestPaths: [
                        "/schools/*/claims/new/*/check_your_answers"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "created_by_id"
                },
                {
                    entityTableName: "claims",
                    requestPaths: [
                        "/schools/*/claims/new/*/check_your_answers"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "submitted_by_id"
                },
                {
                    entityTableName: "schools",
                    requestPaths: [
                        "/schools/*/grant_conditions"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "claims_grant_conditions_accepted_by_id"
                },
                {
                    entityTableName: "claims",
                    requestPaths: [
                        "/support/claims/clawbacks/claims/new/*/*/check_your_answers"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "clawback_requested_by_id"
                },
                {
                    entityTableName: "claim_activities",
                    requestPaths: [
                        "/support/claims/clawbacks/claims/new/*/*/check_your_answers",
                        "/support/claims/sampling/claims/*/reject/new/*/check_your_answers",
                        "/support/claims/sampling/claims/provider_response/new/*/confirmation",
                        "/support/claims/sampling/claims/*",
                        "/support/claims/payments/claims/*/paid",
                        "/support/claims/payments/claims/*/information-sent",
                        "/support/claims/sampling/claims/*/provider_rejected/new/*/check_your_answers",
                        "/support/claims/payments/payer_response/new/*/confirmation",
                        "/support/claims/clawbacks/claims/*/edit/*/*/*/mentor_training_clawback_*",
                        "/support/claims/clawbacks/claims"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "user_id"
                },
                {
                    entityTableName: "payment_responses",
                    requestPaths: [
                        "/support/claims/payments/payer_response/new/*/confirmation"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "user_id"
                }
            ]
        },

        paths: {
            preAuth: [
                "/",
                "/?"
            ],

            signIn: [
                "/auth/dfe/callback",
                "/sign-in"
            ],

            signOut: [
                "/auth/dfe/sign-out",
                "/auth/failure"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/support.*",
                ".*/wp-admin.*",
                ".*/administrator.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    },

    find_placements: {
        startDate: "2025-06-01",

        identity: {
            anchorSources: [{
                    entityTableName: "users",
                    requestPaths: [
                        "/auth/dfe/callback",
                        "/change_organisation/*/update_organisation",
                        "/admin/change_organisation/update_organisation",
                        "/admin/change_organisation/return_to_dashboard"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "placement_preferences",
                    requestPaths: [
                        "/placement_preferences/new/*/school_contact",
                        "/placement_preferences/new/*/check_your_answers",
                        "/placement_preferences/new/*/are_you_sure",
                        "/placement_preferences/new/*/confirm"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "created_by_id"
                }
            ]
        },

        paths: {
            preAuth: [
                "/",
                "/?"
            ],

            signIn: [
                "/sign-in",
                "/login",
                "/auth/dfe/callback"
            ],

            signOut: [
                "/auth/dfe/sign-out",
                "/auth/failure"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/support.*",
                ".*/wp-admin.*",
                ".*/administrator.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    },

    teaching_vacancies: {
        startDate: "2026-04-01",

        identity: {
            anchorSources: [{
                    entityTableName: "jobseekers",
                    requestPaths: [
                        "/jobseekers/auth/govuk_one_login/callback",
                        "/jobseekers/account/email_preferences"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "hidden_data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "job_applications",
                    requestPaths: [
                        "/jobseekers/*/job_application",
                        "/jobseekers/uploaded_job_applications/*/upload_application_form",
                        "/jobseekers/job_applications/*/submit",
                        "/jobseekers/uploaded_job_applications/*/personal_details",
                        "/jobseekers/job_applications/*/build/personal_statement",
                        "/jobseekers/job_applications/*/build/personal_details",
                        "/jobseekers/job_applications/*/build/professional_status",
                        "/jobseekers/job_applications/*/build/equal_opportunities",
                        "/jobseekers/job_applications/*/build/declarations",
                        "/jobseekers/job_applications/*/build/qualifications",
                        "/jobseekers/job_applications/*/build/training_and_cpds",
                        "/jobseekers/job_applications/*/build/employment_history",
                        "/jobseekers/job_applications/*/build/professional_body_memberships",
                        "/jobseekers/job_applications/*/build/referees",
                        "/jobseekers/job_applications/*/build/ask_for_support",
                        "/jobseekers/job_applications/*",
                        "/jobseekers/job_applications/*/build/catholic",
                        "/jobseekers/job_applications/*/withdraw",
                        "/jobseekers/account_transfer",
                        "/jobseekers/job_applications/*/build/non_catholic"
                    ],
                    requestMethods: ["POST"],
                    dataField: "hidden_data",
                    userIdKey: "jobseeker_id"
                },
                {
                    entityTableName: "saved_jobs",
                    requestPaths: [
                        "/jobseekers/*/saved_job/new",
                        "/jobseekers/*.saved_job.*",
                        "/jobseekers/account_transfer"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "hidden_data",
                    userIdKey: "jobseeker_id"
                },
                {
                    entityTableName: "publishers",
                    requestPaths: [
                        "/auth/dfe/callback",
                        "/publishers/ats_interstitial",
                        "/publishers/terms_and_conditions"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "hidden_data",
                    userIdKey: "id"
                },
                {
                    entityTableName: "vacancies",
                    requestPaths: [
                        "/organisation/jobs",
                        "/organisation/jobs/*/publish",
                        "/organisation/jobs/*/convert_to_draft"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "hidden_data",
                    userIdKey: "publisher_id"
                },
                {
                    entityTableName: "feedbacks",
                    requestPaths: [
                        "/jobseekers/job_applications/*/feedback",
                        "/organisation/jobs/*/feedback",
                        "/jobseekers/account/email_preferences",
                        "/jobseekers/account_feedback",
                        "/jobseekers/account_transfer"
                    ],
                    requestMethods: ["POST"],
                    dataField: "hidden_data",
                    userIdKey: "jobseeker_id"
                },
                {
                    entityTableName: "jobseeker_profiles",
                    requestPaths: [
                        "/jobseekers/profile",
                        "/jobseekers/profile/qualified_teacher_status",
                        "/jobseekers/profile/about_you",
                        "/jobseekers/profile/toggle",
                        "/jobseekers/profile/hide_profile/confirm_hide",
                        "/jobseekers/account_transfer"
                    ],
                    requestMethods: ["GET", "POST"],
                    dataField: "hidden_data",
                    userIdKey: "jobseeker_id"
                },
                {
                    entityTableName: "noticed_notifications",
                    requestPaths: [
                        "/jobseekers/job_applications/*",
                        "/organisation/jobs/*/job_applications/*",
                        "/organisation/jobs/*/job_applications/*/messages",
                        "/organisation/jobs/*/job_applications",
                        "/organisation/jobs/*/job_applications/*/self_disclosure"
                    ],
                    requestMethods: ["GET"],
                    dataField: "data",
                    userIdKey: "recipient_id"
                },
                {
                    entityTableName: "messages",
                    requestPaths: [
                        "/organisation/jobs/*/job_applications/*/messages",
                        "/organisation/jobs/*/job_application_batches/*/bulk_rejection_messages/send_messages",
                        "/jobseekers/job_applications/*/messages",
                        "/organisation/jobs/*/job_application_batches/*/bulk_interviewing_messages/send_messages",
                        "/organisation/jobs/*/job_application_batches/*/bulk_shortlisting_messages/send_messages"
                    ],
                    requestMethods: ["POST"],
                    dataField: "data",
                    userIdKey: "sender_id"
                },
                {
                    entityTableName: "publisher_preferences",
                    requestPaths: [
                        "/organisation/jobs"
                    ],
                    requestMethods: ["GET"],
                    dataField: "hidden_data",
                    userIdKey: "publisher_id"
                },
                {
                    entityTableName: "organisation_publishers",
                    requestPaths: [
                        "/auth/dfe/callback"
                    ],
                    requestMethods: ["GET"],
                    dataField: "hidden_data",
                    userIdKey: "publisher_id"
                }
            ]
        },

        paths: {
            preAuth: [
                "/",
                "/?"
            ],

            signIn: [
                "/jobseekers/auth/govuk_one_login/callback",
                "/jobseekers/sign-in",
                "/pages/sign-in",
                "/auth/dfe/callback",
                "/publishers/sign-in"
            ],

            signOut: [
                "/jobseekers/sign_out",
                "/publishers/sign-out"
            ],

            authPrefixes: [
                "/auth/"
            ],

            adminPatterns: [
                ".*/admin.*",
                ".*/wp-admin.*",
                ".*/administrator.*"
            ]
        },

        features: {
            enableAdminNormalisation: true,
            enableJourneyStitching: true,
            enablePreAuthPageStitching: true
        }
    }
};
