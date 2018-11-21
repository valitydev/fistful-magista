/*
 * This file is generated by jOOQ.
 */
package com.rbkmoney.fistful.magista.domain.tables.records;


import com.rbkmoney.fistful.magista.domain.enums.IdentityEventType;
import com.rbkmoney.fistful.magista.domain.tables.IdentityEvent;

import java.time.LocalDateTime;

import javax.annotation.Generated;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record9;
import org.jooq.Row9;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.11.5"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class IdentityEventRecord extends UpdatableRecordImpl<IdentityEventRecord> implements Record9<Long, Long, IdentityEventType, LocalDateTime, LocalDateTime, String, Integer, String, String> {

    private static final long serialVersionUID = -1268268956;

    /**
     * Setter for <code>mst.identity_event.id</code>.
     */
    public void setId(Long value) {
        set(0, value);
    }

    /**
     * Getter for <code>mst.identity_event.id</code>.
     */
    public Long getId() {
        return (Long) get(0);
    }

    /**
     * Setter for <code>mst.identity_event.event_id</code>.
     */
    public void setEventId(Long value) {
        set(1, value);
    }

    /**
     * Getter for <code>mst.identity_event.event_id</code>.
     */
    public Long getEventId() {
        return (Long) get(1);
    }

    /**
     * Setter for <code>mst.identity_event.event_type</code>.
     */
    public void setEventType(IdentityEventType value) {
        set(2, value);
    }

    /**
     * Getter for <code>mst.identity_event.event_type</code>.
     */
    public IdentityEventType getEventType() {
        return (IdentityEventType) get(2);
    }

    /**
     * Setter for <code>mst.identity_event.event_created_at</code>.
     */
    public void setEventCreatedAt(LocalDateTime value) {
        set(3, value);
    }

    /**
     * Getter for <code>mst.identity_event.event_created_at</code>.
     */
    public LocalDateTime getEventCreatedAt() {
        return (LocalDateTime) get(3);
    }

    /**
     * Setter for <code>mst.identity_event.event_occured_at</code>.
     */
    public void setEventOccuredAt(LocalDateTime value) {
        set(4, value);
    }

    /**
     * Getter for <code>mst.identity_event.event_occured_at</code>.
     */
    public LocalDateTime getEventOccuredAt() {
        return (LocalDateTime) get(4);
    }

    /**
     * Setter for <code>mst.identity_event.identity_id</code>.
     */
    public void setIdentityId(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>mst.identity_event.identity_id</code>.
     */
    public String getIdentityId() {
        return (String) get(5);
    }

    /**
     * Setter for <code>mst.identity_event.sequence_id</code>.
     */
    public void setSequenceId(Integer value) {
        set(6, value);
    }

    /**
     * Getter for <code>mst.identity_event.sequence_id</code>.
     */
    public Integer getSequenceId() {
        return (Integer) get(6);
    }

    /**
     * Setter for <code>mst.identity_event.identity_effective_chalenge_id</code>.
     */
    public void setIdentityEffectiveChalengeId(String value) {
        set(7, value);
    }

    /**
     * Getter for <code>mst.identity_event.identity_effective_chalenge_id</code>.
     */
    public String getIdentityEffectiveChalengeId() {
        return (String) get(7);
    }

    /**
     * Setter for <code>mst.identity_event.identity_level_id</code>.
     */
    public void setIdentityLevelId(String value) {
        set(8, value);
    }

    /**
     * Getter for <code>mst.identity_event.identity_level_id</code>.
     */
    public String getIdentityLevelId() {
        return (String) get(8);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public Record1<Long> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record9 type implementation
    // -------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public Row9<Long, Long, IdentityEventType, LocalDateTime, LocalDateTime, String, Integer, String, String> fieldsRow() {
        return (Row9) super.fieldsRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row9<Long, Long, IdentityEventType, LocalDateTime, LocalDateTime, String, Integer, String, String> valuesRow() {
        return (Row9) super.valuesRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Long> field1() {
        return IdentityEvent.IDENTITY_EVENT.ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Long> field2() {
        return IdentityEvent.IDENTITY_EVENT.EVENT_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<IdentityEventType> field3() {
        return IdentityEvent.IDENTITY_EVENT.EVENT_TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<LocalDateTime> field4() {
        return IdentityEvent.IDENTITY_EVENT.EVENT_CREATED_AT;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<LocalDateTime> field5() {
        return IdentityEvent.IDENTITY_EVENT.EVENT_OCCURED_AT;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field6() {
        return IdentityEvent.IDENTITY_EVENT.IDENTITY_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Integer> field7() {
        return IdentityEvent.IDENTITY_EVENT.SEQUENCE_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field8() {
        return IdentityEvent.IDENTITY_EVENT.IDENTITY_EFFECTIVE_CHALENGE_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field9() {
        return IdentityEvent.IDENTITY_EVENT.IDENTITY_LEVEL_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long component1() {
        return getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long component2() {
        return getEventId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventType component3() {
        return getEventType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDateTime component4() {
        return getEventCreatedAt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDateTime component5() {
        return getEventOccuredAt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String component6() {
        return getIdentityId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer component7() {
        return getSequenceId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String component8() {
        return getIdentityEffectiveChalengeId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String component9() {
        return getIdentityLevelId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long value1() {
        return getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long value2() {
        return getEventId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventType value3() {
        return getEventType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDateTime value4() {
        return getEventCreatedAt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDateTime value5() {
        return getEventOccuredAt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value6() {
        return getIdentityId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer value7() {
        return getSequenceId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value8() {
        return getIdentityEffectiveChalengeId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value9() {
        return getIdentityLevelId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value1(Long value) {
        setId(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value2(Long value) {
        setEventId(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value3(IdentityEventType value) {
        setEventType(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value4(LocalDateTime value) {
        setEventCreatedAt(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value5(LocalDateTime value) {
        setEventOccuredAt(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value6(String value) {
        setIdentityId(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value7(Integer value) {
        setSequenceId(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value8(String value) {
        setIdentityEffectiveChalengeId(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord value9(String value) {
        setIdentityLevelId(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IdentityEventRecord values(Long value1, Long value2, IdentityEventType value3, LocalDateTime value4, LocalDateTime value5, String value6, Integer value7, String value8, String value9) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        value6(value6);
        value7(value7);
        value8(value8);
        value9(value9);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached IdentityEventRecord
     */
    public IdentityEventRecord() {
        super(IdentityEvent.IDENTITY_EVENT);
    }

    /**
     * Create a detached, initialised IdentityEventRecord
     */
    public IdentityEventRecord(Long id, Long eventId, IdentityEventType eventType, LocalDateTime eventCreatedAt, LocalDateTime eventOccuredAt, String identityId, Integer sequenceId, String identityEffectiveChalengeId, String identityLevelId) {
        super(IdentityEvent.IDENTITY_EVENT);

        set(0, id);
        set(1, eventId);
        set(2, eventType);
        set(3, eventCreatedAt);
        set(4, eventOccuredAt);
        set(5, identityId);
        set(6, sequenceId);
        set(7, identityEffectiveChalengeId);
        set(8, identityLevelId);
    }
}
