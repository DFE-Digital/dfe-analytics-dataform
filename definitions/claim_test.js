const dfeAnalyticsDataform = require("../");

// Determine if the current month falls within the claims window (September to March)
const currentMonth = new Date().getMonth() + 1; // JavaScript months are 0-indexed, so add 1
const currentDay = new Date().getDay() + 1; // 0-indexed with Sunday as 0; add 1 so Sunday is 1
const isClaimsWindow = currentMonth >= 9 || currentMonth <= 6; //All policies claims window opens in September. Most policies claims window closes in March, apart from IRP which closes in June. 
const isNotDayAfterWeekend = currentDay != 1 && currentDay != 2;

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project - distinguish between them by giving each one a different eventSourceName. This will cause all the tables produced automatically by dfe-analytics-dataform to have your suffix included in them to allow users to tell the difference between them.
dfeAnalyticsDataform({
    disabled: true,
    urlRegex: "www.claim-additional-teaching-payment.service.gov.uk",
    eventSourceName: "claim",
    bqProjectName: "claim-additional-payments",
    bqDatasetName: "claim_events_production",
    bqEventsTableName: "events",
    expirationDays: false,
    enableSessionTables: false,
    enableSessionDetailsTable: true,
    requestPathGroupingRegex: "([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
    hiddenPolicyTagLocation: "projects/claim-additional-payments/locations/europe-west2/taxonomies/6035056991658002237/policyTags/5001017932158544163",
    dataSchema: [{
            entityTableName: "amendments",
            description: "",
            keys: [{
                keyName: "claim_changes",
                dataType: "string",
                description: "Structured data representing the changes made to the claim."
            }, {
                keyName: "claim_id",
                dataType: "string",
                description: "Claim associated with this amendment.",
                foreignKeyTable: "claims"
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who made the amendment."
            }, {
                keyName: "dfe_sign_in_users_id",
                dataType: "string",
                description: "DfE Sign-in user who made the amendment.",
                foreignKeyTable: "dfe_sign_in_users"
            }, {
                keyName: "notes",
                dataType: "string",
                description: "Rationale for the amendment."
            }, {
                keyName: "personal_data_removed_at",
                dataType: "timestamp",
                description: "Timestamp when personal data was removed."
            }]
        },
        {
            entityTableName: "claims",
            description: "",
            dataFreshnessDays: isClaimsWindow && isNotDayAfterWeekend ? 1 : null, // Apply assertion only in claims window and not on days after weekends (which are quiet)
            keys: [{
                keyName: "academic_year",
                dataType: "string",
                description: "Academic year of the claim."
            }, {
                keyName: "first_name",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "middle_name",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "surname",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "date_of_birth",
                dataType: "date",
                description: "",
                hidden: true
            }, {
                keyName: "address_line_1",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "address_line_2",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "address_line_3",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "address_line_4",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "postcode",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "assigned_to_id",
                dataType: "string",
                description: "Admin responsible for processing the claim."
            }, {
                keyName: "bank_or_building_society",
                dataType: "string",
                description: ""
            }, {
                keyName: "claimant_declaration",
                dataType: "boolean",
                description: "True if the claimant confirms the information on the final page of the claim is correct."
            }, {
                keyName: "decision_deadline",
                dataType: "date",
                description: "Date by which DfE should have made a decision about this claim in order to stay within the agreed SLA. See also provider_verification_deadline."
            }, {
                keyName: "eligibility_id",
                dataType: "string",
                description: "Eligibility record associated with the claim."
            }, {
                keyName: "eligibility_type",
                dataType: "string",
                description: "Type of eligibility associated with the claim."
            }, {
                keyName: "email_address",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "email_verified",
                dataType: "boolean",
                description: "True if the email address has been verified by one-time password (OTP)."
            }, {
                keyName: "has_masters_doctoral_loan",
                dataType: "boolean",
                description: "Claimant has a Master's or Doctoral loan."
            }, {
                keyName: "has_student_loan",
                dataType: "boolean",
                description: "Claimant has a student loan."
            }, {
                keyName: "identity_confirmed_with_onelogin",
                dataType: "boolean",
                description: "Claimant’s identity confirmed via GOV.UK One Login."
            }, {
                keyName: "journeys_session_id",
                dataType: "string",
                description: "Journey session associated with the claim."
            }, {
                keyName: "logged_in_with_onelogin",
                dataType: "boolean",
                description: "Claimant signed in via GOV.UK One Login."
            }, {
                keyName: "mobile_verified",
                dataType: "boolean",
                description: "True if the mobile number has been verified by OTP."
            }, {
                keyName: "national_insurance_number",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "onelogin_uid",
                dataType: "string",
                description: "Unique identifier for a user logged in via GOV.UK One Login.",
                hidden: true
            }, {
                keyName: "onelogin_auth_at",
                dataType: "timestamp",
                description: "Timestamp when the user signed in via GOV.UK One Login."
            }, {
                keyName: "onelogin_idv_at",
                dataType: "timestamp",
                description: "Timestamp of the GOV.UK One Login identity verification (regardless of pass or fail)."
            }, {
                keyName: "payroll_gender",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "personal_data_removed_at",
                dataType: "timestamp",
                description: "Timestamp when personal data was removed."
            }, {
                keyName: "policy_options_provided",
                dataType: "string",
                description: ""
            }, {
                keyName: "policy",
                dataType: "string",
                description: "Name of the policy for the claim."
            }, {
                keyName: "postgraduate_doctoral_loan",
                dataType: "boolean",
                description: "Claimant has a postgraduate Doctoral loan."
            }, {
                keyName: "postgraduate_masters_loan",
                dataType: "boolean",
                description: "Claimant has a postgraduate Master’s loan."
            }, {
                keyName: "practitioner_email_address",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "provide_mobile_number",
                dataType: "boolean",
                description: "Whether the claimant will provide a mobile number."
            }, {
                keyName: "provider_contact_name",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "reference",
                dataType: "string",
                description: "Alphanumeric reference for the claim."
            }, {
                keyName: "sent_one_time_password_at",
                dataType: "timestamp",
                description: "Timestamp when the OTP was sent."
            }, {
                keyName: "student_loan_country",
                dataType: "string",
                description: ""
            }, {
                keyName: "student_loan_courses",
                dataType: "string",
                description: ""
            }, {
                keyName: "student_loan_plan",
                dataType: "string",
                description: "Student loan plan type."
            }, {
                keyName: "student_loan_start_date",
                dataType: "date",
                description: ""
            }, {
                keyName: "submitted_at",
                dataType: "timestamp",
                description: "Timestamp when the claim was submitted by the claimant."
            }, {
                keyName: "held",
                dataType: "boolean",
                description: "Claim on hold for processing."
            }, {
                keyName: "hmrc_bank_validation_succeeded",
                dataType: "boolean",
                description: "Whether the claimant’s bank details passed HMRC validation."
            }, {
                keyName: "qa_completed_at",
                dataType: "timestamp",
                description: "Timestamp when quality assurance was completed."
            }, {
                keyName: "qa_required",
                dataType: "boolean",
                description: "Whether the claim requires quality assurance checking."
            }, {
                keyName: "details_check",
                dataType: "boolean",
                description: "Whether the personal details from Teacher ID shown to the user were correct (if false, the claim was submitted manually)."
            }, {
                keyName: "email_address_check",
                dataType: "boolean",
                description: "Whether the email from Teacher ID was correct according to the user."
            }, {
                keyName: "logged_in_with_tid",
                dataType: "boolean",
                description: "Whether the user signed in to Teacher ID rather than entering details manually."
            }, {
                keyName: "mobile_check",
                dataType: "boolean",
                description: "Whether the mobile number from Teacher ID was correct according to the user."
            }, {
                keyName: "qualifications_details_check",
                dataType: "boolean",
                description: "Whether the qualification details from DQT shown to the user were correct (if false, the claim was submitted manually)."
            }, {
                keyName: "started_at",
                dataType: "timestamp",
                description: "Time the claimant clicked 'Start now' (some journeys only)."
            }, {
                keyName: "submitted_using_slc_data",
                dataType: "boolean",
                description: "Determines whether a match was found for the applicant using a copy of the data from the Student Loans Company (SLC)"
            }, {
                keyName: "verified_at",
                dataType: "timestamp",
                description: "Date the claim was verified by the provider."
            }, {
                keyName: "retained_personal_data_removed_at",
                dataType: "timestamp",
                description: "Date and time the claimant’s personal data was removed."
            }, {
                keyName: "onelogin_idv_return_codes",
                dataType: "string",
                description: "Return codes from GOV.UK One Login identity verification.",
                hidden: true
            }]
        },
        {
            entityTableName: "claim_payments",
            description: "",
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: "Claim associated with the payment.",
                foreignKeyTable: "claims"
            }, {
                keyName: "payment_id",
                dataType: "string",
                description: "Payment associated with the claim.",
                foreignKeyTable: "payments"
            }]
        },
        {
            entityTableName: "claimant_flags",
            description: "Claimants whose claims require checking by the ops team. Produced from an uploaded CSV specifying criteria to use to identify these claimants e.g. a NI number. See original commit message: https://github.com/DFE-Digital/claim-additional-payments-for-teaching/commit/b6afd02c2f0fa34b2b72c4fac7020764ae054922",
            keys: [{
                keyName: "policy",
                dataType: "string",
                description: "Claim type (machine readable) this flag applies to",
            }, {
                keyName: "reason",
                dataType: "string",
                description: "Reason why this claimant has been flagged",
            }, {
                keyName: "related_claims",
                dataType: "string",
                description: "JSON containing claim IDs related to this flag",
            }, {
                keyName: "suggested_action",
                dataType: "string",
                description: "Suggested action for ops team to take because of this flag",
            }]
        },
        {
            entityTableName: "decisions",
            description: "",
            dataFreshnessDays: 3,
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: "Claim associated with the decision.",
                foreignKeyTable: "claims"
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who created the decision."
            }, {
                keyName: "notes",
                dataType: "string",
                description: "Reason for the decision."
            }, {
                keyName: "approved",
                dataType: "boolean",
                description: "Whether the claim was approved in this decision."
            }, {
                keyName: "undone",
                dataType: "boolean",
                description: "Whether this decision was reverted."
            }, {
                keyName: "rejected_reasons",
                dataType: "string",
                description: "Rationale for rejecting the claim."
            }]
        },
        {
            entityTableName: "dfe_sign_in_users",
            description: "",
            keys: [{
                keyName: "dfe_sign_in_id",
                dataType: "string",
                description: "External identifier from DfE Sign-in."
            }, {
                keyName: "organisation_name",
                dataType: "string",
                description: "Name of the organisation from DfE Sign-in at login."
            }, {
                keyName: "role_codes",
                dataType: "string",
                description: "Role codes returned by DfE Sign-in."
            }, {
                keyName: "deleted_at",
                dataType: "timestamp",
                description: "Timestamp when the record was soft deleted."
            }, {
                keyName: "user_type",
                dataType: "string",
                description: "Admin or provider, indicating internal support staff or FE provider."
            }, {
                keyName: "current_organisation_ukprn",
                dataType: "string",
                description: "UK Provider Reference Number (UKPRN) for the organisation context at login."
            }]
        },
        {
            entityTableName: "early_career_payments_eligibilities",
            description: "",
            keys: [{
                keyName: "award_amount",
                dataType: "float",
                description: "Award amount."
            }, {
                keyName: "current_school_id",
                dataType: "string",
                description: "School associated with the claim.",
                foreignKeyTable: "schools"
            }, {
                keyName: "eligible_itt_subject",
                dataType: "string",
                description: "Eligible ITT subject."
            }, {
                keyName: "employed_as_supply_teacher",
                dataType: "boolean",
                description: "Claimant employed as a supply teacher."
            }, {
                keyName: "employed_directly",
                dataType: "boolean",
                description: "Claimant employed directly by the school."
            }, {
                keyName: "has_entire_term_contract",
                dataType: "boolean",
                description: "Claimant’s contract covers at least a full term."
            }, {
                keyName: "itt_academic_year",
                dataType: "string",
                description: "Academic year when ITT was completed."
            }, {
                keyName: "nqt_in_academic_year_after_itt",
                dataType: "boolean",
                description: "Claimant teaching as a newly qualified teacher."
            }, {
                keyName: "qualification",
                dataType: "string",
                description: "Route into teaching."
            }, {
                keyName: "subject_to_disciplinary_action",
                dataType: "boolean",
                description: "Claimant subject to disciplinary action."
            }, {
                keyName: "subject_to_formal_performance_action",
                dataType: "boolean",
                description: "Claimant subject to formal performance action."
            }, {
                keyName: "teaching_subject_now",
                dataType: "boolean",
                description: "Claimant teaching an eligible subject now."
            }, {
                keyName: "induction_completed",
                dataType: "boolean",
                description: "Claimant completed induction."
            }, {
                keyName: "school_somewhere_else",
                dataType: "boolean",
                description: "Claimant chose a different school than the one retrieved from TPS (Teacher ID sign-in only)."
            }, {
                keyName: "teacher_reference_number",
                dataType: "string",
                description: "Teacher reference number (TRN).",
                hidden: true
            }]
        },
        {
            entityTableName: "early_years_payment_eligibilities",
            description: "",
            keys: [{
                keyName: "alternative_idv_claimant_bank_details_match",
                dataType: "boolean",
                description: "Claimant’s bank details match those held by the provider."
            }, {
                keyName: "provider_email_address",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "practitioner_first_name",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "practitioner_surname",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "alternative_idv_claimant_date_of_birth",
                dataType: "date",
                description: "",
                hidden: true
            }, {
                keyName: "alternative_idv_claimant_postcode",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "alternative_idv_claimant_national_insurance_number",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "alternative_idv_claimant_email",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "alternative_idv_claimant_employed_by_nursery",
                dataType: "boolean",
                description: "Claimant recognised as an employee of the nursery."
            }, {
                keyName: "alternative_idv_claimant_employment_check_declaration",
                dataType: "boolean",
                description: "Provider confirms the claimant’s identity is correct."
            }, {
                keyName: "alternative_idv_completed_at",
                dataType: "timestamp",
                description: "Timestamp when the alternative identity verification completed."
            }, {
                keyName: "alternative_idv_reference",
                dataType: "string",
                description: "Internal reference used for magic links."
            }, {
                keyName: "award_amount",
                dataType: "float",
                description: "Award amount."
            }, {
                keyName: "child_facing_confirmation_given",
                dataType: "string",
                description: "Confirmation that the claimant mostly performs child-facing duties."
            }, {
                keyName: "nursery_urn",
                dataType: "string",
                description: "URN of the nursery."
            }, {
                keyName: "practitioner_claim_started_at",
                dataType: "timestamp",
                description: "Timestamp when the practitioner started the claim."
            }, {
                keyName: "practitioner_reminder_email_sent_count",
                dataType: "integer",
                description: "Count of reminder emails sent to the practitioner."
            }, {
                keyName: "practitioner_reminder_email_last_sent_at",
                dataType: "timestamp",
                description: "Timestamp of the last reminder sent to the practitioner."
            }, {
                keyName: "provider_claim_submitted_at",
                dataType: "timestamp",
                description: "Timestamp when the provider submitted the claim."
            }, {
                keyName: "provider_entered_contract_type",
                dataType: "string",
                description: "Contract type entered by the provider."
            }, {
                keyName: "provider_six_month_employment_reminder_sent_at",
                dataType: "timestamp",
                description: "Timestamp when the six‑month employment reminder was sent to the provider."
            }, {
                keyName: "returner_contract_type",
                dataType: "string",
                description: "Contract type for the returner."
            }, {
                keyName: "returner_worked_with_children",
                dataType: "boolean",
                description: "Claimant previously worked directly with children."
            }, {
                keyName: "returning_within_6_months",
                dataType: "boolean",
                description: "Claimant worked in early years within the past six months."
            }, {
                keyName: "start_date",
                dataType: "date",
                description: "Start date for the claimant’s role."
            }]
        },
        {
            entityTableName: "eligible_ey_providers",
            description: "",
            keys: [{
                keyName: "nursery_name",
                dataType: "string",
                description: "Name of the nursery."
            }, {
                keyName: "urn",
                dataType: "string",
                description: "URN of the nursery."
            }, {
                keyName: "local_authority_id",
                dataType: "string",
                description: "Local authority associated with the nursery."
            }, {
                keyName: "max_claims",
                dataType: "string",
                description: "Maximum permitted claims for the nursery."
            }, {
                keyName: "nursery_address",
                dataType: "string",
                description: "Address of the nursery."
            }]
        },
        {
            entityTableName: "eligible_fe_providers",
            description: "",
            keys: [{
                keyName: "academic_year",
                dataType: "string",
                description: "Academic year associated with the eligibility record."
            }, {
                keyName: "primary_key_contact_email_address",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "lower_award_amount",
                dataType: "float",
                description: "Lower award amount."
            }, {
                keyName: "max_award_amount",
                dataType: "float",
                description: "Maximum award amount."
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: "UK Provider Reference Number (UKPRN)."
            }]
        },
        {
            entityTableName: "events",
            description: "Events that took place in the lifecycle of a Claim, forming an audit trail.",
            keys: [{
                    keyName: "name",
                    dataType: "string",
                    description: "Name of the type of event that happened."
                },
                {
                    keyName: "actor_id",
                    dataType: "string",
                    description: "UID of the DfE Sign-in user who caused the event",
                    foreignKeyTable: "dfe_sign_in_users"
                },
                {
                    keyName: "claim_id",
                    dataType: "string",
                    description: "UID of the claim associated with the event.",
                    foreignKeyTable: "claims"
                },
                {
                    keyName: "entity_type",
                    alias: "event_entity_type",
                    /* Because entity_id is a dfe-analytics-dataform reserved word */
                    dataType: "string",
                    description: "Name of the Ruby class for another entity (not the claim or actor) that the event was also associated with."
                },
                {
                    keyName: "entity_id",
                    alias: "event_entity_id",
                    /* Because entity_id is a dfe-analytics-dataform reserved word */
                    dataType: "string",
                    description: "UID of another entity (not the claim) associated with the event."
                }
            ]
        },
        {
            entityTableName: "further_education_payments_eligibilities",
            description: "",
            keys: [{
                keyName: "award_amount",
                dataType: "float",
                description: "Award amount."
            }, {
                keyName: "building_construction_courses",
                dataType: "string",
                description: "Building and construction courses taught."
            }, {
                keyName: "chemistry_courses",
                dataType: "string",
                description: "Chemistry courses taught."
            }, {
                keyName: "claimant_identity_verified_at",
                dataType: "timestamp",
                description: "Timestamp when the claimant’s identity was verified."
            }, {
                keyName: "computing_courses",
                dataType: "string",
                description: "Computing courses taught."
            }, {
                keyName: "contract_type",
                dataType: "string",
                description: "Claimant’s contract type with the provider."
            }, {
                keyName: "early_years_courses",
                dataType: "string",
                description: "Early years courses taught."
            }, {
                keyName: "engineering_manufacturing_courses",
                dataType: "string",
                description: "Engineering and manufacturing courses taught."
            }, {
                keyName: "fixed_term_full_year",
                dataType: "boolean",
                description: "Fixed-term contract covers the full academic year."
            }, {
                keyName: "flagged_as_duplicate",
                dataType: "boolean",
                description: "Possible duplicate claim."
            }, {
                keyName: "flagged_as_mismatch_on_teaching_start_year",
                dataType: "boolean",
                description: "Mismatch or conflict on the further‑education teaching start year."
            }, {
                keyName: "flagged_as_previously_start_year_matches_claim_false",
                dataType: "boolean",
                description: "TRUE if this claim has been flagged as a previously rejected claim where provider start year did not match claimant start year"
            }, {
                keyName: "further_education_teaching_start_year",
                dataType: "string",
                description: "Academic year the claimant started FE teaching."
            }, {
                keyName: "half_teaching_hours",
                dataType: "boolean",
                description: "Claimant teaches relevant groups for at least half of teaching hours."
            }, {
                keyName: "hours_teaching_eligible_subjects",
                dataType: "boolean",
                description: "Claimant spends sufficient time teaching eligible subjects."
            }, {
                keyName: "maths_courses",
                dataType: "string",
                description: "Maths courses taught."
            }, {
                keyName: "physics_courses",
                dataType: "string",
                description: "Physics courses taught."
            }, {
                keyName: "possible_school_id",
                dataType: "string",
                description: "Possible school identified during the search."
            }, {
                keyName: "provider_assigned_to_id",
                dataType: "string",
                description: "Provider user assigned to verify the claim.",
                foreignKeyTable: "dfe_sign_in_users"
            }, {
                keyName: "provider_entered_contract_type",
                dataType: "string",
                description: "Contract type entered by the provider.",
                historic: true
            }, {
                keyName: "provider_six_month_employment_reminder_sent_at",
                dataType: "timestamp",
                description: "Timestamp when the six‑month employment reminder was sent to the provider.",
                historic: true
            }, {
                keyName: "provider_verification_actual_subjects_taught",
                dataType: "string",
                description: "Subjects taught, as verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_building_construction_courses",
                dataType: "string",
                description: "Building and construction courses taught, verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_chemistry_courses",
                dataType: "string",
                description: "Chemistry courses taught, verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_chase_email_last_sent_at",
                dataType: "timestamp",
                description: "Timestamp of the last follow‑up email sent to the provider."
            }, {
                keyName: "provider_verification_claimant_bank_details_match",
                dataType: "boolean",
                description: "Claimant’s bank details match those held by the provider."
            }, {
                keyName: "provider_verification_claimant_date_of_birth",
                dataType: "date",
                description: "Claimant’s date of birth (as entered by the provider).",
                hidden: true
            }, {
                keyName: "provider_verification_claimant_email",
                dataType: "string",
                description: "Claimant’s work email address (as entered by the provider).",
                hidden: true
            }, {
                keyName: "provider_verification_claimant_employed_by_college",
                dataType: "boolean",
                description: "Claimant employed by the provider."
            }, {
                keyName: "provider_verification_claimant_employment_check_declaration",
                dataType: "boolean",
                description: "Provider agrees with the claimant’s employment details."
            }, {
                keyName: "provider_verification_claimant_national_insurance_number",
                dataType: "string",
                description: "Claimant’s National Insurance number (as entered by the provider).",
                hidden: true
            }, {
                keyName: "provider_verification_claimant_postcode",
                dataType: "string",
                description: "Claimant’s postcode (as entered by the provider).",
                hidden: true
            }, {
                keyName: "claimant_date_of_birth",
                dataType: "date",
                description: "",
                hidden: true
            }, {
                keyName: "claimant_postcode",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "claimant_national_insurance_number",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "provider_verification_completed_at",
                dataType: "timestamp",
                description: "Timestamp when provider verification completed."
            }, {
                keyName: "provider_verification_contract_covers_full_academic_year",
                dataType: "boolean",
                description: "Provider confirms the contract covers the full academic year."
            }, {
                keyName: "provider_verification_contract_type",
                dataType: "string",
                description: "Contract type according to the provider."
            }, {
                keyName: "provider_verification_computing_courses",
                dataType: "string",
                description: "Computing courses taught, verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_deadline",
                dataType: "date",
                description: "Deadline date by which a provider should verify a claim. See also decision_deadline."
            }, {
                keyName: "provider_verification_declaration",
                dataType: "boolean",
                description: "Provider declaration checked."
            }, {
                keyName: "provider_verification_disciplinary_action",
                dataType: "boolean",
                description: "Provider indicates disciplinary measures against the claimant."
            }, {
                keyName: "provider_verification_early_years_courses",
                dataType: "string",
                description: "Early years courses taught, verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_email_count",
                dataType: "integer",
                description: "Number of verification emails sent to the provider."
            }, {
                keyName: "provider_verification_email_last_sent_at",
                dataType: "timestamp",
                description: "Timestamp when the provider was last sent an email."
            }, {
                keyName: "provider_verification_engineering_manufacturing_courses",
                dataType: "string",
                description: "Engineering and manufacturing courses taught, verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_half_teaching_hours",
                dataType: "boolean",
                description: "Provider indicates the claimant teaches relevant courses for at least half of teaching hours."
            }, {
                keyName: "provider_verification_half_timetabled_teaching_time",
                dataType: "boolean",
                description: "Provider indicates the claimant teaches relevant courses for at least half of timetabled time."
            }, {
                keyName: "provider_verification_in_first_five_years",
                dataType: "boolean",
                description: "Provider confirms the claimant is within the first five years of teaching.",
                historic: true
            }, {
                keyName: "provider_verification_maths_courses",
                dataType: "string",
                description: "Maths courses taught, verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_not_started_qualification_reason_other",
                dataType: "string",
                description: "Further detail when the 'other' reason is selected for not starting a qualification."
            }, {
                keyName: "provider_verification_not_started_qualification_reasons",
                dataType: "string",
                description: "Reasons the practitioner has not started a qualification (as provided by the provider)."
            }, {
                keyName: "provider_verification_performance_measures",
                dataType: "boolean",
                description: "Provider indicates the claimant meets performance measures."
            }, {
                keyName: "provider_verification_physics_courses",
                dataType: "string",
                description: "Physics courses taught, verified by the provider.",
                historic: true
            }, {
                keyName: "provider_verification_started_at",
                dataType: "timestamp",
                description: "Timestamp when provider verification started."
            }, {
                keyName: "provider_verification_teaching_start_year",
                dataType: "string",
                description: "Academic year the provider says the claimant started FE teaching"
            }, {
                keyName: "provider_verification_teaching_start_year_matches_claim",
                dataType: "string",
                historic: true,
                description: "Agreement status on whether the teaching start year matches the claim."
            }, {
                keyName: "provider_verification_subjects_taught",
                dataType: "boolean",
                description: "Provider indicates the subjects shown match those the claimant teaches.",
                historic: true
            }, {
                keyName: "provider_verification_taught_at_least_one_academic_term",
                dataType: "boolean",
                description: "Provider indicates the claimant has taught for at least one academic term."
            }, {
                keyName: "provider_verification_teaching_hours_per_week",
                dataType: "string",
                description: "Teaching hours per week according to the provider."
            }, {
                keyName: "provider_verification_teaching_qualification",
                dataType: "string",
                description: "Teaching qualification according to the provider."
            }, {
                keyName: "provider_verification_teaching_responsibilities",
                dataType: "boolean",
                description: "Provider confirms the claimant has teaching responsibilities."
            }, {
                keyName: "provider_verification_timetabled_teaching_hours",
                dataType: "boolean",
                description: "Provider confirms the claimant is timetabled to teach at least 2.5 hours per week for the relevant term.",
                historic: true
            }, {
                keyName: "provider_verification_verified_by_id",
                dataType: "string",
                description: "Provider user who verified the claim.",
                foreignKeyTable: "dfe_sign_in_users"
            }, {
                keyName: "provision_search",
                dataType: "string",
                description: "Search text used to find the provider."
            }, {
                keyName: "school_id",
                dataType: "string",
                description: "School selected by the claimant."
            }, {
                keyName: "subject_to_disciplinary_action",
                dataType: "boolean",
                description: "Claimant subject to disciplinary action."
            }, {
                keyName: "subject_to_formal_performance_action",
                dataType: "boolean",
                description: "Claimant subject to formal performance action."
            }, {
                keyName: "subjects_taught",
                dataType: "string",
                description: "Subjects taught by the claimant."
            }, {
                keyName: "taught_at_least_one_term",
                dataType: "boolean",
                description: "Claimant has taught for at least one term."
            }, {
                keyName: "teacher_reference_number",
                dataType: "string",
                description: "Teacher reference number (TRN).",
                hidden: true
            }, {
                keyName: "teaching_hours_per_week",
                dataType: "string",
                description: "Teaching hours per week."
            }, {
                keyName: "teaching_hours_per_week_next_term",
                dataType: "string",
                description: "",
                historic: true
            }, {
                keyName: "teaching_qualification",
                dataType: "string",
                description: "Teaching qualification held by the claimant."
            }, {
                keyName: "teaching_responsibilities",
                dataType: "string",
                description: "Teaching responsibilities held by the claimant."
            }, {
                keyName: "valid_passport",
                dataType: "boolean",
                description: "Claimant holds a valid passport."
            }, {
                keyName: "work_email",
                dataType: "string",
                description: "Claimant’s work email address (collected if One Login identity verification fails).",
                hidden: true
            }, {
                keyName: "work_email_verified",
                dataType: "boolean",
                description: "True if the work email address was verified by OTP."
            }]
        },

        {
            entityTableName: "international_relocation_payments_eligibilities",
            description: "",
            keys: [{
                keyName: "application_route",
                dataType: "string",
                description: "Employment status route: teacher, salaried trainee, or other."
            }, {
                keyName: "award_amount",
                dataType: "float",
                description: "Award amount."
            }, {
                keyName: "breaks_in_employment",
                dataType: "boolean",
                description: "Claimant had breaks in employment."
            }, {
                keyName: "current_school_id",
                dataType: "string",
                description: "School where the claimant is currently employed."
            }, {
                keyName: "date_of_entry",
                dataType: "string",
                description: "Date the claimant moved to England to start the teaching job."
            }, {
                keyName: "employment_history",
                dataType: "string",
                description: "Claimant’s employment history (JSON)."
            }, {
                keyName: "nationality",
                dataType: "string",
                description: "Claimant’s nationality."
            }, {
                keyName: "one_year",
                dataType: "boolean",
                description: "Claimant employed on a contract lasting at least one year."
            }, {
                keyName: "start_date",
                dataType: "string",
                description: "Start date of the claimant’s contract."
            }, {
                keyName: "state_funded_secondary_school",
                dataType: "boolean",
                description: "Claimant employed by an English state secondary school."
            }, {
                keyName: "subject",
                dataType: "string",
                description: "Subject the claimant is employed to teach (e.g., Physics, Languages)."
            }, {
                keyName: "changed_workplace_or_new_contract",
                dataType: "boolean",
                description: "Claimant changed workplace or started a new contract in the past year."
            }, {
                keyName: "previous_year_claim_ids",
                dataType: "string",
                description: "Claim IDs from the previous year."
            }, {
                keyName: "visa_type",
                dataType: "string",
                description: "Visa used to move to England."
            }]
        },
        {
            entityTableName: "journeys_sessions",
            description: "",
            keys: [{
                keyName: "journey",
                dataType: "string",
                description: "Which journey the session is for."
            }, {
                keyName: "expired",
                dataType: "boolean",
                description: "True if the journey session has expired."
            }, {
                keyName: "steps",
                dataType: "string",
                description: "Array of steps taken in the journey (JSONB)."
            }]
        },
        {
            entityTableName: "targeted_retention_incentive_payments_eligibilities",
            description: "",
            keys: [{
                keyName: "award_amount",
                dataType: "float",
                description: "Award amount."
            }, {
                keyName: "current_school_id",
                dataType: "string",
                description: "School associated with the claim.",
                foreignKeyTable: "schools"
            }, {
                keyName: "eligible_degree_subject",
                dataType: "boolean",
                description: "Claimant holds a degree in an eligible subject."
            }, {
                keyName: "eligible_itt_subject",
                dataType: "string",
                description: "Eligible ITT subject."
            }, {
                keyName: "employed_as_supply_teacher",
                dataType: "boolean",
                description: "Claimant employed as a supply teacher."
            }, {
                keyName: "employed_directly",
                dataType: "boolean",
                description: "Claimant employed directly by the school."
            }, {
                keyName: "has_entire_term_contract",
                dataType: "boolean",
                description: "Contract covers an entire term or longer."
            }, {
                keyName: "itt_academic_year",
                dataType: "string",
                description: "Academic year when ITT was completed."
            }, {
                keyName: "nqt_in_academic_year_after_itt",
                dataType: "boolean",
                description: "Claimant teaching as a newly qualified teacher."
            }, {
                keyName: "qualification",
                dataType: "string",
                description: "Route into teaching."
            }, {
                keyName: "subject_to_disciplinary_action",
                dataType: "boolean",
                description: "Claimant subject to disciplinary action."
            }, {
                keyName: "subject_to_formal_performance_action",
                dataType: "boolean",
                description: "Claimant subject to formal performance action."
            }, {
                keyName: "teaching_subject_now",
                dataType: "boolean",
                description: "Claimant teaching an eligible subject now."
            }, {
                keyName: "induction_completed",
                dataType: "boolean",
                description: "Claimant completed induction."
            }, {
                keyName: "school_somewhere_else",
                dataType: "boolean",
                description: "Claimant chose a different school than the one retrieved from TPS (Teacher ID sign-in only)."
            }, {
                keyName: "teacher_reference_number",
                dataType: "string",
                description: "Teacher reference number (TRN).",
                hidden: true
            }]
        },
        {
            entityTableName: "targeted_retention_incentive_payments_awards",
            description: "",
            keys: [{
                keyName: "academic_year",
                dataType: "string",
                description: "Academic year for the award."
            }, {
                keyName: "award_amount",
                dataType: "float",
                description: "Award amount."
            }, {
                keyName: "school_urn",
                dataType: "string",
                description: "School URN associated with the award."
            }]
        },
        {
            entityTableName: "local_authorities",
            description: "",
            keys: [{
                keyName: "code",
                dataType: "integer",
                description: "External local authority code."
            }, {
                keyName: "name",
                dataType: "string",
                description: "Local authority name."
            }]
        },
        {
            entityTableName: "local_authority_districts",
            description: "",
            keys: [{
                keyName: "code",
                dataType: "string",
                description: "External local authority district code."
            }, {
                keyName: "name",
                dataType: "string",
                description: "Local authority district name."
            }]
        },
        {
            entityTableName: "notes",
            description: "",
            keys: [{
                keyName: "body",
                dataType: "string",
                description: "Note text."
            }, {
                keyName: "claim_id",
                dataType: "string",
                description: "Claim associated with the note.",
                foreignKeyTable: "claims"
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who created the note."
            }, {
                keyName: "important",
                dataType: "boolean",
                description: "Whether the note is marked as important."
            }, {
                keyName: "label",
                dataType: "string",
                description: "Category label for the note."
            }]
        },
        {
            entityTableName: "payments",
            description: "",
            keys: [{
                keyName: "award_amount",
                dataType: "float",
                description: "Award amount."
            }, {
                keyName: "employers_national_insurance",
                dataType: "float",
                description: "Employer’s National Insurance paid."
            }, {
                keyName: "gross_pay",
                dataType: "float",
                description: "Gross pay before deductions."
            }, {
                keyName: "gross_value",
                dataType: "float",
                description: "Gross value of the payment."
            }, {
                keyName: "national_insurance",
                dataType: "float",
                description: "Employee National Insurance paid."
            }, {
                keyName: "net_pay",
                dataType: "float",
                description: "Net pay after deductions."
            }, {
                keyName: "payroll_reference",
                dataType: "string",
                description: "Payroll confirmation reference."
            }, {
                keyName: "payroll_run_id",
                dataType: "string",
                description: "Payroll run associated with the payment.",
                foreignKeyTable: "payroll_runs"
            }, {
                keyName: "postgraduate_loan_repayment",
                dataType: "float",
                description: "Postgraduate loan repayment."
            }, {
                keyName: "student_loan_repayment",
                dataType: "float",
                description: "Student loan repayment."
            }, {
                keyName: "tax",
                dataType: "float",
                description: "Income tax paid on the payment."
            }, {
                keyName: "confirmation_id",
                dataType: "string",
                description: "Payment confirmation associated with the payment.",
                foreignKeyTable: "payment_confirmations"
            }, {
                keyName: "scheduled_payment_date",
                dataType: "date",
                description: "Scheduled payment date."
            }]
        },
        {
            entityTableName: "payment_confirmations",
            description: "",
            keys: [{
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who created the payment confirmation."
            }, {
                keyName: "payroll_run_id",
                dataType: "string",
                description: "Payroll run associated with the confirmation.",
                foreignKeyTable: "payroll_runs"

            }]
        },

        {
            entityTableName: "payroll_runs",
            description: "",
            keys: [{
                keyName: "confirmation_report_uploaded_by_id",
                dataType: "string",
                description: "Admin who uploaded the confirmation report."
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who created the payroll run."
            }, {
                keyName: "downloaded_at",
                dataType: "timestamp",
                description: "Timestamp when the payroll run was downloaded."
            }, {
                keyName: "downloaded_by_id",
                dataType: "string",
                description: "Admin who downloaded the payroll run."
            }, {
                keyName: "scheduled_payment_date",
                dataType: "date",
                description: "Scheduled payment date."
            }, {
                keyName: "status",
                dataType: "string",
                description: "Payroll run status (defaults to pending)."
            }]
        },
        {
            entityTableName: "journey_configurations",
            primaryKey: "routing_name",
            description: "",
            keys: [{
                keyName: "availability_message",
                dataType: "string",
                description: "Optional message shown when the journey is closed."
            }, {
                keyName: "current_academic_year",
                dataType: "string",
                description: "Configured academic year for the journey."
            }, {
                keyName: "open_for_submissions",
                dataType: "boolean",
                description: "Whether the journey accepts submissions."
            }, {
                keyName: "teacher_id_enabled",
                dataType: "boolean",
                description: "Whether Teacher ID sign‑in is enabled.",
            }, {
                keyName: "routing_name",
                dataType: "string",
                description: "Path used in the URL for the journey.",
            }]
        },
        {
            entityTableName: "reminders",
            description: "",
            keys: [{
                keyName: "email_sent_at",
                dataType: "timestamp",
                description: "Timestamp when the reminder email was sent."
            }, {
                keyName: "full_name",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "email_address",
                dataType: "string",
                description: "",
                hidden: true
            }, {
                keyName: "email_verified",
                dataType: "boolean",
                description: "True if the email address was verified by OTP."
            }, {
                keyName: "itt_academic_year",
                dataType: "string",
                description: "ITT academic year relevant to the reminder."
            }, {
                keyName: "itt_subject",
                dataType: "string",
                description: "ITT subject relevant to the reminder."
            }, {
                keyName: "journey_class",
                dataType: "string",
                description: "Journey associated with the reminder."
            }, {
                keyName: "sent_one_time_password_at",
                dataType: "timestamp",
                description: "Timestamp when the OTP was sent."
            }, {
                keyName: "deleted_at",
                dataType: "timestamp",
                description: "Timestamp when the reminder was soft deleted."
            }]
        },
        {
            entityTableName: "schools",
            description: "",
            keys: [{
                keyName: "close_date",
                dataType: "date",
                description: "School close date."
            }, {
                keyName: "county",
                dataType: "string",
                description: "County."
            }, {
                keyName: "establishment_number",
                dataType: "integer",
                description: "Establishment number."
            }, {
                keyName: "local_authority_district_id",
                dataType: "string",
                description: "Local authority district associated with the school.",
                foreignKeyTable: "local_authority_districts"
            }, {
                keyName: "local_authority_id",
                dataType: "string",
                description: "Local authority associated with the school.",
                foreignKeyTable: "local_authorities"
            }, {
                keyName: "locality",
                dataType: "string",
                description: "Locality."
            }, {
                keyName: "name",
                dataType: "string",
                description: "School name."
            }, {
                keyName: "open_date",
                dataType: "date",
                description: "School open date."
            }, {
                keyName: "phase",
                dataType: "string",
                description: "School phase (GIAS)."
            }, {
                keyName: "phone_number",
                dataType: "string",
                description: "School contact number (GIAS)."
            }, {
                keyName: "postcode",
                dataType: "string",
                description: "Postcode."
            }, {
                keyName: "postcode_sanitised",
                dataType: "string",
                description: ""
            }, {
                keyName: "school_type",
                dataType: "string",
                description: "School type (GIAS)."
            }, {
                keyName: "school_type_group",
                dataType: "string",
                description: "School type group (GIAS)."
            }, {
                keyName: "statutory_high_age",
                dataType: "integer",
                description: "Statutory high age (GIAS)."
            }, {
                keyName: "street",
                dataType: "string",
                description: "Street."
            }, {
                keyName: "town",
                dataType: "string",
                description: "Town."
            }, {
                keyName: "urn",
                dataType: "integer",
                description: "School URN."
            }, {

                keyName: "ukprn",
                dataType: "string",
                description: "UK Provider Reference Number (UKPRN) – issued by the UK Register of Learning Providers."
            }]
        },
        {
            entityTableName: "student_loans_eligibilities",
            description: "",
            keys: [{
                keyName: "biology_taught",
                dataType: "boolean",
                description: "Claimant teaches Biology."
            }, {
                keyName: "chemistry_taught",
                dataType: "boolean",
                description: "Claimant teaches Chemistry."
            }, {
                keyName: "claim_school_id",
                dataType: "string",
                description: "School sourced from existing data.",
                foreignKeyTable: "schools"
            }, {
                keyName: "computing_taught",
                dataType: "boolean",
                description: "Claimant teaches Computing."
            }, {
                keyName: "current_school_id",
                dataType: "string",
                description: "Current school if different from the sourced school.",
                foreignKeyTable: "schools"
            }, {
                keyName: "employment_status",
                dataType: "string",
                description: "Employment status (sourced school or elsewhere)."
            }, {
                keyName: "had_leadership_position",
                dataType: "boolean",
                description: "Claimant holds a leadership position."
            }, {
                keyName: "languages_taught",
                dataType: "boolean",
                description: "Claimant teaches Languages."
            }, {
                keyName: "mostly_performed_leadership_duties",
                dataType: "boolean",
                description: "Claimant mostly performed leadership duties."
            }, {
                keyName: "physics_taught",
                dataType: "boolean",
                description: "Claimant teaches Physics."
            }, {
                keyName: "qts_award_year",
                dataType: "string",
                description: "Year the teacher attained Qualified Teacher Status (QTS)."
            }, {
                keyName: "student_loan_repayment_amount",
                dataType: "float",
                description: ""
            }, {
                keyName: "award_amount",
                dataType: "float",
                description: "Amount the claimant is claiming."
            }, {
                keyName: "taught_eligible_subjects",
                dataType: "boolean",
                description: "Claimant teaches an eligible subject."
            }, {
                keyName: "claim_school_somewhere_else",
                dataType: "boolean",
                description: "Claimant is claiming against a different school."
            }, {
                keyName: "teacher_reference_number",
                dataType: "string",
                description: "Teacher reference number (TRN).",
                hidden: true
            }]
        },
        {
            entityTableName: "support_tickets",
            description: "",
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: "Claim associated with the support ticket.",
                foreignKeyTable: "claims"
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who created the support ticket."
            }, {
                keyName: "url",
                dataType: "string",
                description: "Ticket URL."
            }]
        },
        {
            entityTableName: "tasks",
            description: "",
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: "Claim associated with the task.",
                foreignKeyTable: "claims"
            }, {
                keyName: "claim_verifier_match",
                dataType: "string",
                description: "Match status for details included in the task."
            }, {
                keyName: "data",
                dataType: "string",
                description: "Additional metadata stored for the task (JSON)."
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who created the task."
            }, {
                keyName: "manual",
                dataType: "boolean",
                description: "Task created manually rather than automatically."
            }, {
                keyName: "name",
                dataType: "string",
                description: "Task type name."
            }, {
                keyName: "passed",
                dataType: "boolean",
                description: "Task passed status."
            }, {
                keyName: "reason",
                dataType: "string",
                description: "Reason the task passed or failed. Some records may contain the string 'no_data' to represent failed One Login checks where identity data could not be returned."
            }]
        },
        {
            entityTableName: "topups",
            description: "Top up payments are made in March when the claim window closes. Payment is made for claims that initially received an incorrect payment. ",
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: "Claim associated with the top‑up.",
                foreignKeyTable: "claims"
            }, {
                keyName: "payment_id",
                dataType: "string",
                description: "Payment associated with the top‑up.",
                foreignKeyTable: "payments"
            }, {
                keyName: "award_amount",
                dataType: "float",
                description: "Top‑up award amount."
            }, {
                keyName: "dfe_sign_in_users_id",
                dataType: "string",
                description: "DfE Sign-in user who created the top‑up.",
                foreignKeyTable: "dfe_sign_in_users"
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "Admin who created the top‑up."
            }]
        },
        {
            entityTableName: "school_workforce_censuses",
            description: "",
            keys: [{
                keyName: "teacher_reference_number",
                dataType: "string",
                description: "Teacher reference number (TRN).",
                hidden: true
            }, {
                keyName: "school_urn",
                dataType: "string",
                description: "School URN."
            }, {
                keyName: "totfte",
                dataType: "string",
                description: ""
            }, {
                keyName: "contract_agreement_type",
                dataType: "string",
                description: "Teacher’s contract agreement type."
            }, {
                keyName: "subject_description_sfr",
                dataType: "string",
                description: ""
            }, {
                keyName: "general_subject_code",
                dataType: "string",
                description: ""
            }, {
                keyName: "hours_taught",
                dataType: "integer",
                description: "Hours taught by the teacher."
            }]
        },
        {
            entityTableName: "stats",
            description: "",
            keys: [{
                keyName: "one_login_return_code",
                dataType: "string",
                description: "Return code from GOV.UK One Login."
            }, {
                keyName: "type",
                dataType: "string",
                description: "One Login code type."
            }]
        },
        {
            entityTableName: "teachers_pensions_service",
            description: "",
            keys: [{
                keyName: "teacher_reference_number",
                dataType: "string",
                description: "Teacher reference number (TRN).",
                hidden: true
            }, {
                keyName: "start_date",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "end_date",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "la_urn",
                dataType: "string",
                description: "Local authority URN."
            }, {
                keyName: "school_urn",
                dataType: "string",
                description: "School URN."
            }, {
                keyName: "employer_id",
                dataType: "string",
                description: "Employer identifier."
            }, {
                keyName: "nino",
                dataType: "string",
                description: "National Insurance number.",
                hidden: true
            }, {
                keyName: "gender_digit",
                dataType: "string",
                description: "Gender indicator digit.",
                hidden: true
            }]
        }
    ]
});
