const dfeAnalyticsDataform = require("../");

dfeAnalyticsDataform({
    disabled: true,
    eventSourceName: "afqts",
    bqProjectName: "apply-for-qts-in-england",
    bqDatasetName: "events_production",
    bqEventsTableName: "events",
    expirationDays: false,
    enableWebRequestIdentityResolution: true,
    urlRegex: "apply-for-qts-in-england.education.gov.uk",
    hiddenPolicyTagLocation: "projects/apply-for-qts-in-england/locations/europe-west2/taxonomies/5456044749211275650/policyTags/2399328962407973209",
    dataSchema: [
        {
            entityTableName: "application_forms",
            description: "Application information about applicants and their current status",
            keys: [
                {
                    keyName: "action_required_by",
                    dataType: "string",
                    description: "Action that needs to be taken by an admin, assessor, or external organisation or person on an application",
                },
                {
                    keyName: "age_range_max",
                    dataType: "string",
                    description: "Pseudonymised maximum age range that can be taught",
                },
                {
                    keyName: "age_range_min",
                    dataType: "string",
                    description: "Pseudonymised minimum age range that can be taught",
                },
                {
                    keyName: "age_range_status",
                    dataType: "string",
                    description: "Status of application having age range information",
                },
                {
                    keyName: "alternative_family_name",
                    dataType: "string",
                    description: "Pseudonymised data",
                    hidden: true,
                },
                {
                    keyName: "alternative_given_names",
                    dataType: "string",
                    description: "Pseudonymised data",
                    hidden: true,
                },
                {
                    keyName: "assessor_id",
                    dataType: "string",
                    description: "ID of the staff user who is assigned to the application",
                },
                {
                    keyName: "awarded_at",
                    dataType: "timestamp",
                    description: "When a recommendation to award has been applied",
                },
                {
                    keyName: "confirmed_no_sanctions",
                    dataType: "boolean",
                    description: "Whether the applicant has confirmed they have no sanctions",
                },
                {
                    keyName: "date_of_birth",
                    dataType: "date",
                    description: "Pseudonymised data",
                    hidden: true,
                },
                {
                    keyName: "declined_at",
                    dataType: "timestamp",
                    description: "When a recommendation to decline has been applied",
                },
                {
                    keyName: "trs_match",
                    dataType: "string",
                    description: "Any matches with an existing teacher recorded in TRS are stored here",
                    hidden: true,
                    pastKeyNames: ['dqt_match']
                },
                {
                    keyName: "eligibility_check_id",
                    dataType: "string",
                    description: "Eligibility check ID - This will be the link to the eligibility check record that the applicant used to generate the application form",
                },
                {
                    keyName: "english_language_citizenship_exempt",
                    dataType: "boolean",
                    description: "Applicant is from one of the exempt countries. Therefore only needs to prove citizenship",
                },
                {
                    keyName: "english_language_proof_method",
                    dataType: "string",
                    description: "Applicant was taught in English in a non exempt country. Therefore, they provide a MOI (Medium of Instruction)",
                },
                {
                    keyName: "english_language_provider_id",
                    dataType: "string",
                    description: "English test provider ID. e.g SELT",
                },
                {
                    keyName: "english_language_provider_other",
                    dataType: "boolean",
                    description: "Applicant uses an English test provider not from the standard list. This is typically seen and accepted for Ukraine citizenship holders",
                },
                {
                    keyName: "english_language_provider_reference",
                    dataType: "string",
                    description: "Pseudonymised data",
                },
                {
                    keyName: "english_language_qualification_exempt",
                    dataType: "boolean",
                    description: "Applicant is able to prove they have been taught in one of the selected exempt countries",
                },
                {
                    keyName: "english_language_status",
                    dataType: "string",
                    description: "Status of application having English language",
                },
                {
                    keyName: "family_name",
                    dataType: "string",
                    description: "Pseudonymised data",
                    hidden: true,
                },
                {
                    keyName: "given_names",
                    dataType: "string",
                    description: "Pseudonymised data",
                    hidden: true,
                },
                {
                    keyName: "has_alternative_name",
                    dataType: "string",
                    description: "Pseudonymised data",
                },
                {
                    keyName: "has_other_england_work_history",
                    dataType: "boolean",
                    description: "Application forms where applicant has indicated that they have other work experience in England",
                },
                {
                    keyName: "has_work_history",
                    dataType: "string",
                    description: "Pseudonymised data",
                },
                {
                    keyName: "includes_prioritisation_features",
                    dataType: "boolean",
                    description: "Application forms whether the prioritisation features for including other work experience in England is present",
                },
                {
                    keyName: "identification_document_status",
                    dataType: "string",
                    description: "Identification document status",
                },
                {
                    keyName: "national_insurance_number",
                    dataType: "string",
                    description: "The UK National insurance number optionally provided by applicants when submitting their application. This attribute was only available from 19th November 2025.",
                    hidden: true,
                },
                {
                    keyName: "needs_registration_number",
                    dataType: "boolean",
                    description: "Applicants from specific countries need to provide a number for their teaching status.",
                },
                {
                    keyName: "needs_work_history",
                    dataType: "boolean",
                    description: "Applicants from specific countries do not need to provide work history. i.e. NI, Scotland, Ukraine",
                },
                {
                    keyName: "needs_written_statement",
                    dataType: "boolean",
                    description: "If there isn't an online checker for competent authority, then written statment checks need to be uploaded",
                },
                {
                    keyName: "other_england_work_history_status",
                    dataType: "string",
                    description: "Application form where the other work experience in England task is completed while in draft",
                },
                {
                    keyName: "overdue_further_information",
                    dataType: "boolean",
                    description: "Further information has not be supplied within the allotted time frame. Overdue FI is likely to lead to a decline.",
                    historic: true,
                },
                {
                    keyName: "overdue_professional_standing",
                    dataType: "boolean",
                    description: "When applicant is in verification status, check is being done with competent authority to verify evidence provided and it has gone overdue (post 6wks)",
                    historic: true,
                },
                {
                    keyName: "overdue_qualification",
                    dataType: "boolean",
                    description: "When applicant is in verification status, check is being done with Ecctis to verify qualifications provided and it has gone overdue (post 6wks)",
                    historic: true,
                },
                {
                    keyName: "overdue_reference",
                    dataType: "boolean",
                    description: "When applicant is in verification status, check is being done with references provided to verify work history and it has gone overdue (post 6wks)",
                    historic: true,
                },
                {
                    keyName: "passport_country_of_issue_code",
                    dataType: "string",
                    description: "Country of issue of passport provided by applicant",
                    hidden: true,
                },
                {
                    keyName: "passport_document_status",
                    dataType: "string",
                    description: "passport document provided status",
                },
                {
                    keyName: "passport_expiry_date",
                    dataType: "date",
                    description: "Passport expiry date provided",
                    hidden: true,
                },
                {
                    keyName: "personal_information_status",
                    dataType: "string",
                    description: "personal information provided status",
                },
                {
                    keyName: "qualification_changed_work_history_duration",
                    dataType: "boolean",
                    description: "If true applicant changed their qualification certificate date which meant the number of months of work history they added has changed, if false the applicant has subsequently changed their work history",
                },
                {
                    keyName: "qualifications_status",
                    dataType: "string",
                    description: "Qualification provided status",
                },
                {
                    keyName: "received_further_information",
                    dataType: "boolean",
                    description: "Requested further information has been received. If this goes overdue, applicant will likely be marked as decline",
                    historic: true,
                },
                {
                    keyName: "received_professional_standing",
                    dataType: "boolean",
                    description: "In verification status, we contact professional standing body Received professional standing checker",
                    historic: true,
                },
                {
                    keyName: "received_qualification",
                    dataType: "boolean",
                    description: "Received relevant qualification",
                    historic: true,
                },
                {
                    keyName: "received_reference",
                    dataType: "boolean",
                    description: "Received references",
                    historic: true,
                },
                {
                    keyName: "reduced_evidence_accepted",
                    dataType: "boolean",
                    description: "Reduced evidence accepted",
                },
                {
                    keyName: "reference",
                    dataType: "integer",
                    description: "Application reference Id",
                },
                {
                    keyName: "region_id",
                    dataType: "string",
                    description: "Region ID",
                },
                {
                    keyName: "registration_number",
                    dataType: "string",
                    description: "Pseudonymised data",
                    hidden: true,
                },
                {
                    keyName: "registration_number_status",
                    dataType: "string",
                    description: "Teacher Registration number where applicable has been received",
                },
                {
                    keyName: "requires_passport_as_identity_proof",
                    dataType: "boolean",
                    description: "requires passport as identity proof",
                },
                {
                    keyName: "requires_private_email_for_referee",
                    dataType: "boolean",
                    description: "Requires private email domain for referees when submitting application form",
                },
                {
                    keyName: "requires_preliminary_check",
                    dataType: "boolean",
                    description: "requires preliminary check",
                },
                {
                    keyName: "reviewer_id",
                    dataType: "string",
                    description: "ID of the staff user who is assigned as the reviewer",
                },
                {
                    keyName: "stage",
                    dataType: "string",
                    description: "Lifecycle of the application journey - draft, pre_assessment, not_started, assessment, verification, review or completed",
                },
                {
                    keyName: "started_with_private_email_for_referee",
                    dataType: "boolean",
                    description: "Started application form after the private email domain for referee requirement was released. Note that this is a temporary column to handle some features for existing applications and will be removed within the next year.",
                },
                {
                    keyName: "status",
                    dataType: "string",
                    description: "",
                    alias: "state",
                    historic: true,
                },
                {
                    keyName: "statuses",
                    dataType: "string",
                    description: "Status or statuses an application is in",
                },
                {
                    keyName: "subject_limited",
                    dataType: "boolean",
                    description: "Whether this application is subject to restricted subjects criteria",
                },
                {
                    keyName: "subjects",
                    dataType: "string",
                    description: " Pseudonymised data ",
                },
                {
                    keyName: "subjects_status",
                    dataType: "string",
                    description: " Subjects submitted status ",
                },
                {
                    keyName: "submitted_at",
                    dataType: "timestamp",
                    description: " Date / time application was submitted ",
                },
                {
                    keyName: "teacher_id",
                    dataType: "string",
                    description: "Specific to AQTS. It is not the same as the cross-service teacher_id and cannot be used for linking teachers across services.",
                },
                {
                    keyName: "teaching_authority_provides_written_statement",
                    dataType: "boolean",
                    description: " If there isn't an online checker for competent authority, then written statment checks need to be uploaded",
                },
                {
                    keyName: "teaching_qualification_part_of_degree",
                    dataType: "boolean",
                    description: "A boolean indicating whether the teaching qualification is part of the bachelor’s degree. If true applicant needs to provide only 1 (combined degree or qualification).If false then the applicant needs to tell us about 2 qualifications (their teaching and their degree) ",
                },
                {
                    keyName: "waiting_on_further_information",
                    dataType: "boolean",
                    description: " Requested further information is pending. If this goes overdue, applicant will likely be marked as decline ",
                    historic: true,
                },
                {
                    keyName: "waiting_on_professional_standing",
                    dataType: "boolean",
                    description: " We are waiting on confirmation from professional standing body regarding professional standing checker",
                    historic: true,
                },
                {
                    keyName: "waiting_on_qualification",
                    dataType: "boolean",
                    description: " Waiting on qualification checker",
                    historic: true,
                },
                {
                    keyName: "waiting_on_reference",
                    dataType: "boolean",
                    description: "Waiting on reference checker",
                    historic: true,
                },
                {
                    keyName: "withdrawn_at",
                    dataType: "timestamp",
                    description: " Date / time applicant has withdrawn ",
                },
                {
                    keyName: "work_history_status",
                    dataType: "string",
                    description: "Application forms work history spoke status",
                },
                {
                    keyName: "working_days_between_submitted_and_today",
                    dataType: "integer",
                    description: "Working days between application form being been submitted until today",
                },
                {
                    keyName: "working_days_between_submitted_and_completed",
                    dataType: "integer",
                    description: "Working days between application form being submitted until awarded, declined or withdrawn ",
                },
                {
                    keyName: "written_statement_confirmation",
                    dataType: "boolean",
                    description: " Written statement confirmation checker",
                },
                {
                    keyName: "written_statement_optional",
                    dataType: "boolean",
                    description: " Written statement optional checker",
                },
                {
                    keyName: "written_statement_status",
                    dataType: "string",
                    description: " Written statement status",
                },
            ],
        },
        {
            entityTableName: "application_holds",
            description: "Information on an application hold against an application form",
            keys: [
                {
                    keyName: "reason",
                    dataType: "string",
                    description: "The reason the application hold was created",
                },
                {
                    keyName: "reason_comment",
                    dataType: "string",
                    description: "If the reason is 'other' then a comment is provided",
                    hidden: true,
                },
                {
                    keyName: "release_comment",
                    dataType: "string",
                    description: "Comment on why the hold was released",
                    hidden: true,
                },
                {
                    keyName: "application_form_id",
                    dataType: "string",
                    description: "ID of the application form the hold is for",
                },
                {
                    keyName: "released_at",
                    dataType: "timestamp",
                    description: "When the application hold was released",
                },
            ],
        },
        {
            entityTableName: "assessment_sections",
            description: "Table documenting status per assessment section",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "Assessment Section ID",
                },
                {
                    keyName: "checks",
                    dataType: "string",
                    description: "Checks conducted during assessment",
                },
                {
                    keyName: "failure_reasons",
                    dataType: "string",
                    description: "If false, applicant did not fail. If true, reason for applicant failure",
                },
                {
                    keyName: "key",
                    dataType: "string",
                    description: "What applicant was assessed on",
                },
                {
                    keyName: "passed",
                    dataType: "boolean",
                    description: "If true, applicant has passed the section they are assessed on. If false, applicant has not passed selected assessed area",
                },
                {
                    keyName: "preliminary",
                    dataType: "boolean",
                    description: "f true, preliminary section to be assessed. If false, non preliminary section assessed",
                },
                {
                    keyName: "assessed_at",
                    dataType: "timestamp",
                    description: "When the section was last assessed",
                },
            ],
        },
        {
            entityTableName: "assessments",
            description: "Table documenting the assessment process for an applicant",
            keys: [
                {
                    keyName: "age_range_max",
                    dataType: "integer",
                    description: "the assessor chooses what age range the applicant can teach after looking through their qualifications",
                },
                {
                    keyName: "age_range_min",
                    dataType: "integer",
                    description: "the assessor chooses what age range the applicant can teach after looking through their qualifications",
                },
                {
                    keyName: "application_form_id",
                    dataType: "string",
                    description: "Application form Id",
                },
                {
                    keyName: "induction_required",
                    dataType: "boolean",
                    description: "If true, induction is required. If false, induction is not required",
                },
                {
                    keyName: "recommendation",
                    dataType: "string",
                    description: "category of recommendation. Value of 'unknown' is applied when an auto decline has been triggered, e.g. LOPS overdue ",
                },
                {
                    keyName: "recommended_at",
                    dataType: "timestamp",
                    description: "When a decision is made to award or decline. Note that awarded_at and declined_at represent when the award/decline recxommendation is applied.",
                },
                                {
                    keyName: "recommendation_assessor_note",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "references_verified",
                    dataType: "boolean",
                    description: "If true, references are verified. If false, they are not",
                },
                {
                    keyName: "scotland_full_registration",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "started_at",
                    dataType: "timestamp",
                    description: "When an assessor assesses any part of the application. For applications which require a preliminary check, that includes the preliminary check",
                },
                {
                    keyName: "verification_started_at",
                    dataType: "timestamp",
                    description: "When an assessor starts the verification stage on an assessment",
                },
                {
                    keyName: "subjects",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "unsigned_consent_document_generated",
                    dataType: "boolean",
                    description: "indicates the assessor has generated the unsigned consent document ready to be sent to the applicant",
                },
                {
                    keyName: "working_days_between_started_and_today",
                    dataType: "integer",
                    description: "Working days between assessment being started and today",
                },
                {
                    keyName: "working_days_between_started_and_completed",
                    dataType: "integer",
                    description: "Working days between assessment being started and application form being awarded, declined or withdrawn",
                },
                {
                    keyName: "working_days_between_submitted_and_started",
                    dataType: "integer",
                    description: "Working days between application form being submitted and assessment started",
                },
                {
                    keyName: "working_days_between_started_and_verification_started",
                    dataType: "integer",
                    description: "Working days between assessment started and assessment moving into verification started",
                },
                {
                    keyName: "working_days_between_submitted_and_verification_started",
                    dataType: "integer",
                    description: "Working days between application submitted and assessment moving into verification started",
                },
                {
                    keyName: "working_days_between_submitted_and_prioritisation_decision",
                    dataType: "integer",
                    description: "Working days between application submitted and assessment having a prioritisation decision made (prioritisation_decision_at being present)",
                },
                {
                    keyName: "prioritisation_decision_at",
                    dataType: "timestamp",
                    description: "This represents the timestamp at which the prioritisation decision was made at (regardless of result). This is only relevant for assessments that go through prioritisation checks.",
                },
                {
                    keyName: "prioritised",
                    dataType: "boolean",
                    description: "This represents whether an assessment is prioritised or not. This is only relevant for assessments that go through prioritisation checks. This will remain as 'null' if assessment has not gone through prioritisation checks.",
                },
            ],
        },
        {
            entityTableName: "consent_requests",
            description: "",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "ID of the assessment that the consent request belongs to.",
                },
                {
                    keyName: "expired_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "qualification_id",
                    dataType: "string",
                    description: "ID of the qualification that the consent request belongs to.",
                },
                {
                    keyName: "received_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "requested_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reviewed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "review_passed",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "unsigned_document_downloaded",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "verified_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "verify_passed",
                    dataType: "string",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "countries",
            description: "",
            keys: [
                {
                    keyName: "code",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "eligibility_enabled",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "eligibility_skip_questions",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "subject_limited",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "teaching_authority_address",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "teaching_authority_emails",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "teaching_authority_websites",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "teaching_authority_certificate",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "teaching_authority_other",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "teaching_authority_name",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
            ],
        },
        {
            entityTableName: "export_audits",
            description: "A table representing all the data exports made by staff members",
            keys: [
                {
                    keyName: "export_type",
                    dataType: "string",
                    description: "The type of export. e.g. application_forms.",
                },
                {
                    keyName: "exported_by_id",
                    dataType: "string",
                    description: "The ID of the staff member who exported data.",
                },
                {
                    keyName: "filter_params",
                    dataType: "string",
                    description: "An object holding the filter parameters that were used to export the data.",
                    hidden: true,
                },
            ],
        },
        {
            entityTableName: "documents",
            description: "",
            keys: [
                {
                    keyName: "available",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "document_type",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "documentable_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "documentable_type",
                    dataType: "string",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "eligibility_checks",
            description: "",
            keys: [
                {
                    keyName: "completed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "country_code",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "degree",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "free_of_sanctions",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "qualification",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "qualified_for_subject",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "region_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teach_children",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "work_experience",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "work_experience_referee",
                    dataType: "boolean",
                    description: "Whether user has a valid referee to verify their work experience",
                },
                {
                    keyName: "eligible_work_experience_in_england",
                    dataType: "boolean",
                    description: "Whether user has 'valid educational setting in England within the last 12 months?' during eligibility checker for prioritisation",
                },
            ],
        },
        {
            entityTableName: "eligibility_domains",
            description: "",
            keys: [
                {
                    keyName: "archived_at",
                    dataType: "timestamp",
                    description: "When an eligibility domain has been archived, we indicate this by setting this timestamp",
                },
                {
                    keyName: "domain",
                    dataType: "string",
                    description: "This is the domain which is a unique identifier for this record",
                },
                {
                    keyName: "application_forms_count",
                    dataType: "integer",
                    description: "This is the number of application forms that include one or more work histories that include a referee with this domain",
                },
                {
                    keyName: "created_by_id",
                    dataType: "string",
                    description: "The ID of the staff member that created this record",
                },
            ],
        },
        {
            entityTableName: "email_deliveries",
            description: "A list of email deliveries for application forms. The tracking of email deliveries went live on 26th November 2025.",
            keys: [
                {
                    keyName: "application_form_id",
                    dataType: "string",
                    description: "The ID of the application_form the email delivery belongs to.",
                },
                {
                    keyName: "to",
                    dataType: "string",
                    description: "The email address the email was sent to.",
                    hidden: true,
                },
                {
                    keyName: "further_information_request_id",
                    dataType: "string",
                    description: "The ID of the further_information_request the email delivery belongs to (optional).",
                },
                {
                    keyName: "mailer_action_name",
                    dataType: "string",
                    description: "The name of the action for the Mailer. e.g. application_received.",
                },
                {
                    keyName: "mailer_class_name",
                    dataType: "string",
                    description: "The name of the mailer used to send the email. e.g. referee_mailer.",
                },
                {
                    keyName: "notify_completed_at",
                    dataType: "timestamp",
                    description: "The timestamp GOV.UK Notify either delivered or considered the delivery as a failure. This column is only relevant for email deliveries from 2nd March 2026.",
                },
                {
                    keyName: "notify_id",
                    dataType: "string",
                    description: "The ID (UUID) of the delivery on GOV.UK Notify. This column is only relevant for email deliveries from 2nd March 2026.",
                },
                {
                    keyName: "notify_status",
                    dataType: "string",
                    description: "The status of the delivery on GOV.UK Notify. This column is only relevant for email deliveries from 2nd March 2026.",
                },
                {
                    keyName: "prioritisation_reference_request_id",
                    dataType: "string",
                    description: "The ID of the prioritisation_reference_request the email delivery belongs to (optional).",
                },
                {
                    keyName: "reference_request_id",
                    dataType: "string",
                    description: "The ID of the reference_request the email delivery belongs to (optional).",
                },
                {
                    keyName: "subject",
                    dataType: "string",
                    description: "The subject of the email delivered.",
                    hidden: true,
                },
            ],
        },
        {
            entityTableName: "english_language_providers",
            description: "",
            keys: [
                {
                    keyName: "accepted_tests",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "b2_level_requirement",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "b2_level_requirement_prefix",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "check_url",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "name",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "other_information",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "reference_hint",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "reference_name",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "url",
                    dataType: "string",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "further_information_request_items",
            description: "",
            keys: [
                {
                    keyName: "failure_reason_assessor_feedback",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "contact_email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "contact_job",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "contact_name",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "failure_reason_key",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "further_information_request_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "information_type",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "review_decision",
                    dataType: "string",
                    description: "Review decision on further information item. Feature live from 9th April 2025",
                },
                {
                    keyName: "review_decision_note",
                    dataType: "string",
                    description: "Review decision note on further information item where even more further information is requested. Feature live from 9th April 2025",
                    hidden: true,
                },
                {
                    keyName: "work_history_id",
                    dataType: "string",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "further_information_requests",
            description: "",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "expired_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "review_passed",
                    dataType: "boolean",
                    description: "",
                    pastKeyNames: ['passed'],
                },
                {
                    keyName: "received_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "requested_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reviewed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "state",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "working_days_between_assessment_started_to_requested",
                    dataType: "integer",
                    description: "Working days between assessment started and further information request being requested",
                },
            ],
        },
        {
            entityTableName: "mail_delivery_failures",
            description: "Table documenting email delivery failure",
            keys: [
                {
                    keyName: "email_address",
                    dataType: "string",
                    description: "Email address to which the email was sent",
                    hidden: true,
                },
                {
                    keyName: "mailer_action_method",
                    dataType: "string",
                    description: "The method in the mailer class that was used to send the email",
                },
                {
                    keyName: "mailer_class",
                    dataType: "string",
                    description: "The class responsible for sending the email",
                },
            ],
        },
        {
            entityTableName: "notes",
            description: "",
            keys: [
                {
                    keyName: "application_form_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "eligibility_domain_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "author_id",
                    dataType: "string",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "professional_standing_requests",
            description: "",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "expired_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "review_passed",
                    dataType: "boolean",
                    description: "",
                    pastKeyNames: ['passed'],
                },
                {
                    keyName: "ready_for_review",
                    dataType: "boolean",
                    description: "",
                    historic: true,

                },
                {
                    keyName: "received_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "requested_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reviewed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "state",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "verified_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "verify_passed",
                    dataType: "boolean",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "prioritisation_reference_requests",
            description: "For any assessments that have gone through initial prioritisation work history checks and passed, will have prioritisation reference requests sent for those work histories.",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "An assessment can has multiple prioritisation_reference_requests.",
                },
                {
                    keyName: "work_history_id",
                    dataType: "string",
                    description: "This column will be unique against all records.",
                },
                {
                    keyName: "prioritisation_work_history_check_id",
                    dataType: "string",
                    description: "This column will be unique against all records.",
                },
                {
                    keyName: "contact_response",
                    dataType: "boolean",
                    description: "Response of the referee indicating whether the information is correct or not about the referee.",
                },
                {
                    keyName: "confirm_applicant_response",
                    dataType: "boolean",
                    description: "Response of the referee indicating whether the information is correct or not about the applicant and their role.",
                },
                {
                    keyName: "requested_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "received_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "expired_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "review_passed",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "reviewed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "review_note",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
            ],
        },
        {
            entityTableName: "prioritisation_work_history_checks",
            description: "This represents a prioritisation check on an assessment for any work history that is in England and within the last 12 months of being submitted. Any assessment that has this record, indicates that they're going through or have gone through prioritisation checks.",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "An assessment can has multiple prioritisation_work_history_checks.",
                },
                {
                    keyName: "work_history_id",
                    dataType: "string",
                    description: "This column will be unique against all records.",
                },
                {
                    keyName: "checks",
                    dataType: "string",
                    description: "Array of checks conducted during prioritisation role and setting check",
                },
                {
                    keyName: "failure_reasons",
                    dataType: "string",
                    description: "Array of potential failure reasons while conducting prioritisation role and setting checks",
                },
                {
                    keyName: "passed",
                    dataType: "boolean",
                    description: "The result of whether the prioritisation work history check has passed or not. When null, this represent the check being incomplete.",
                },
            ],
        },
        {
            entityTableName: "qualification_requests",
            description: "",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "expired_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "review_passed",
                    dataType: "boolean",
                    description: "",
                    pastKeyNames: ['passed'],
                },
                {
                    keyName: "qualification_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "received_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "requested_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reviewed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "consent_method",
                    dataType: "string",
                    description: "Indicates whether a request for signed consent is required for qualification request. Unknown is the default before an assessor has chosen consent method; unsigned indicates signed consent is not required, signed_ecctis indicates signed consent is required and consent document is from Ecctis; signed_institution indicates signed consent is required and signed consent is from the institution; none indicates no consent is required.",
                },
                {
                    keyName: "signed_consent_document_required",
                    dataType: "boolean",
                    description: "indicates whether a request for signed consent is required for qualification request - replaced by consent_method.",
                    historic: true
                },

                {
                    keyName: "verified_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "verify_passed",
                    dataType: "boolean",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "qualifications",
            description: "",
            keys: [
                {
                    keyName: "application_form_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "certificate_date",
                    dataType: "string",
                    description: "Date on teaching certificate",
                },
                {
                    keyName: "complete_date",
                    dataType: "string",
                    description: "Date completed teaching qualification",
                },
                {
                    keyName: "institution_country_code",
                    dataType: "string",
                    description: "Country code of institution providing teaching qualification",
                },
                {
                    keyName: "institution_name",
                    dataType: "string",
                    description: "Institution providing teaching qualification",
                },
                {
                    keyName: "part_of_university_degree",
                    dataType: "boolean",
                    description: "Teaching qualification part of university degree",
                    historic: true,
                },
                {
                    keyName: "start_date",
                    dataType: "string",
                    description: "Date started teaching qualification",
                },
                {
                    keyName: "title",
                    dataType: "string",
                    description: "Title of teaching qualification",
                },
            ],
        },
        {
            entityTableName: "reference_requests",
            description: "",
            keys: [
                {
                    keyName: "assessment_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "children_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "contact_job",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "contact_name",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "contact_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "dates_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "excludes_suitability_and_concerns_question",
                    dataType: "boolean",
                    description: "When true, this reference request excludes the suitability and concerns questions. Set to true for requests created after 22 October 2025, false for legacy requests",
                },
                {
                    keyName: "expired_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "hours_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "lessons_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "misconduct_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "review_passed",
                    dataType: "boolean",
                    description: "",
                    pastKeyNames: ['passed'],
                },
                {
                    keyName: "received_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reports_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "requested_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reviewed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "satisfied_response",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "state",
                    dataType: "string",
                    description: "",
                    historic: true,
                },
                {
                    keyName: "verified_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "verify_passed",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "work_history_id",
                    dataType: "string",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "regions",
            description: "",
            keys: [
                {
                    keyName: "application_form_skip_work_history",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "country_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "name",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "requires_preliminary_check",
                    dataType: "boolean",
                    description: "",
                    alias: "regions_requires_preliminary_check",
                },
                {
                    keyName: "reduced_evidence_accepted",
                    dataType: "boolean",
                    description: "",
                    alias: "regions_reduced_evidence_accepted",
                },
                {
                    keyName: "sanction_check",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "status_check",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teaching_authority_certificate",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teaching_authority_emails",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teaching_authority_address",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teaching_authority_name",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teaching_authority_online_checker_url",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teaching_authority_provides_written_statement",
                    dataType: "string",
                    description: "",
                    alias: "regions_teaching_authority_provides_written_statement",
                },
                {
                    keyName: "teaching_authority_requires_submission_email",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "teaching_authority_websites",
                    dataType: "string",
                    description: "",
                    pastKeyNames: ['teaching_authority_website']
                },
                {
                    keyName: "written_statement_optional",
                    dataType: "boolean",
                    description: "",
                    alias: "regions_written_statement_optional",
                },
            ],
        },
        {
            entityTableName: "reminder_emails",
            description: "Contains reminder email data and polymorphic links to other tables",
            keys: [
                {
                    keyName: "remindable_id",
                    dataType: "string",
                    description: "Part of a polymorphic foreign key. Indicates the ID of the table specified in remindable_type",
                },
                {
                    keyName: "remindable_type",
                    dataType: "string",
                    description: "Part of polymorphic foreign key. Indicates the table that the remindable_id links to",
                },
                {
                    keyName: "name",
                    dataType: "string",
                    description: " Used when there are different kinds of reminder emails that can be sent. The default type of email is 'expiration' which indicates a reminder email being sent before the object expires (i.e. before expired_at is set). However, application forms have a 'references' type of email which is sent to the applicant if any reference requests are due to expire soon to try and get them to prompt their referees. The reason it is on the application form is the service sends a single email combining all the outstanding reference requests together,rather than one email per reference request.",
                },
            ],
        },
        {
            entityTableName: "selected_failure_reasons",
            description: "The selected failure reasons by assessor either during prioritisation work history check or during specific assessment sections",
            keys: [
                {
                    keyName: "assessment_section_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "prioritisation_work_history_check_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "key",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "assessor_feedback",
                    dataType: "string",
                    description: "The feedback from the assessor about why this failure reason was selected.",
                    hidden: true,
                },
            ],
        },
        {
            entityTableName: "staff",
            description: "",
            keys: [
                {
                    keyName: "archived",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "assess_permission",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "change_email_permission",
                    dataType: "boolean",
                    description: "Whether the user has permission to change the application email address.",
                },
                {
                    keyName: "change_name_permission",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "change_work_history_and_qualification_permission",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "confirmation_sent_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "confirmed_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "current_sign_in_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "current_sign_in_ip",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "failed_attempts",
                    dataType: "integer",
                    description: "",
                },
                {
                    keyName: "invitation_accepted_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "invitation_created_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "invitation_limit",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "invitation_sent_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "invitations_count",
                    dataType: "integer",
                    description: "",
                },
                {
                    keyName: "invited_by_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "invited_by_type",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "last_sign_in_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "last_sign_in_ip",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "locked_at",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "name",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "remember_created_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reset_password_sent_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "reverse_decision_permission",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "sign_in_count",
                    dataType: "integer",
                    description: "",
                },
                {
                    keyName: "support_console_permission",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "manage_staff_permission",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "unconfirmed_email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "verify_permission",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "withdraw_permission",
                    dataType: "boolean",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "support_requests",
            description: "The list of support requests submitted by users",
            keys: [
                {
                    keyName: "application_enquiry_type",
                    dataType: "string",
                    description: "The enquiry type related to requests that are from applicants with submitted applications.",
                },
                {
                    keyName: "application_reference",
                    dataType: "string",
                    description: "The application reference related to requests that are from applicants with submitted applications.",
                },
                {
                    keyName: "user_type",
                    dataType: "string",
                    description: "The type user submitting the request. This can be either of application_submitted, submitting_an_application, providing_a_reference or other.",
                },
                {
                    keyName: "comment",
                    dataType: "string",
                    description: "The comment about the enquiry.",
                    hidden: true,
                },
                {
                    keyName: "email",
                    dataType: "string",
                    description: "The email of the user submitting the request.",
                    hidden: true,
                },
                {
                    keyName: "name",
                    dataType: "string",
                    description: "The name of the user submitting the request.",
                    hidden: true,
                },
                {
                    keyName: "submitted_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "zendesk_ticket_created_at",
                    dataType: "timestamp",
                    description: "The timestamp the request was created on Zendesk.",
                },
                {
                    keyName: "zendesk_ticket_id",
                    dataType: "string",
                    description: "The ticket ID relating to the request on Zendesk.",
                },
            ],
        },
        {
            entityTableName: "suitability_record_emails",
            description: "",
            keys: [
                {
                    keyName: "canonical",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "suitability_record_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "value",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
            ],
        },
        {
            entityTableName: "suitability_record_names",
            description: "",
            keys: [
                {
                    keyName: "suitability_record_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "value",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
            ],
        },
        {
            entityTableName: "suitability_records",
            description: "",
            keys: [
                {
                    keyName: "archived_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "archived_by_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "country_code",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "created_by_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "date_of_birth",
                    dataType: "date",
                    description: "",
                    hidden: true,
                },
            ],
        },
        {
            entityTableName: "teachers",
            description: "",
            keys: [
                {
                    keyName: "canonical_email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "current_sign_in_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "current_sign_in_ip",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "email_domain",
                    dataType: "string",
                    description: "The domain part of the email address (right side of the @).",
                },
                {
                    keyName: "gov_one_email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "last_sign_in_at",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "last_sign_in_ip",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "sign_in_count",
                    dataType: "integer",
                    description: "",
                },
                {
                    keyName: "trn",
                    dataType: "integer",
                    description: "",
                },
                {
                    keyName: "uuid",
                    dataType: "string",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "trs_trn_requests",
            description: "",
            keys: [
                {
                    keyName: "application_form_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "potential_duplicate",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "request_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "state",
                    dataType: "string",
                    description: "",

                },
            ],
        },
        {
            entityTableName: "uploads",
            description: "",
            keys: [
                {
                    keyName: "document_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "filename",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "malware_scan_result",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "translation",
                    dataType: "boolean",
                    description: "",
                },
            ],
        },
        {
            entityTableName: "work_histories",
            description: "",
            keys: [
                {
                    keyName: "address_line1",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "address_line2",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "application_form_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "eligibility_domain_id",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "canonical_contact_email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "city",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "contact_email",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "contact_email_domain",
                    dataType: "string",
                    description: "The domain part of the contact email address (right side of the @).",
                },
                {
                    keyName: "contact_job",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "contact_name",
                    dataType: "string",
                    description: "",
                    hidden: true,
                },
                {
                    keyName: "country_code",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "end_date",
                    dataType: "timestamp",
                    description: "",
                },
                {
                    keyName: "end_date_is_estimate",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "hours_per_week",
                    dataType: "integer",
                    description: "",
                },
                {
                    keyName: "is_other_england_educational_role",
                    dataType: "boolean",
                    description: "This determines work histories which were entered as part of 'Other work experience in England' which do not count towards their experience and only used for prioritisation of application forms",
                },
                {
                    keyName: "job",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "postcode",
                    dataType: "string",
                    description: "Postal code of the school (Released 2nd June 2025)",
                },
                {
                    keyName: "school_name",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "school_website",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "start_date",
                    dataType: "string",
                    description: "",
                },
                {
                    keyName: "start_date_is_estimate",
                    dataType: "boolean",
                    description: "",
                },
                {
                    keyName: "still_employed",
                    dataType: "string",
                    description: "",
                },
            ],

        },
    ],
     customEventSchema: [{
            eventType: "form_validation_failure",
            description: "Occasions when the applicant receives a form validation failure",
            keys: []
        }

    ]
});
