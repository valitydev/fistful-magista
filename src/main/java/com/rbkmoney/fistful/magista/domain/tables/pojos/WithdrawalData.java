/*
 * This file is generated by jOOQ.
 */
package com.rbkmoney.fistful.magista.domain.tables.pojos;


import java.io.Serializable;
import java.util.UUID;

import javax.annotation.Generated;


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
public class WithdrawalData implements Serializable {

    private static final long serialVersionUID = 1307997043;

    private Long   id;
    private UUID   partyId;
    private String identityId;
    private String withdrawalId;
    private String walletId;
    private String destinationId;
    private Long   amount;
    private String currencyCode;

    public WithdrawalData() {}

    public WithdrawalData(WithdrawalData value) {
        this.id = value.id;
        this.partyId = value.partyId;
        this.identityId = value.identityId;
        this.withdrawalId = value.withdrawalId;
        this.walletId = value.walletId;
        this.destinationId = value.destinationId;
        this.amount = value.amount;
        this.currencyCode = value.currencyCode;
    }

    public WithdrawalData(
        Long   id,
        UUID   partyId,
        String identityId,
        String withdrawalId,
        String walletId,
        String destinationId,
        Long   amount,
        String currencyCode
    ) {
        this.id = id;
        this.partyId = partyId;
        this.identityId = identityId;
        this.withdrawalId = withdrawalId;
        this.walletId = walletId;
        this.destinationId = destinationId;
        this.amount = amount;
        this.currencyCode = currencyCode;
    }

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UUID getPartyId() {
        return this.partyId;
    }

    public void setPartyId(UUID partyId) {
        this.partyId = partyId;
    }

    public String getIdentityId() {
        return this.identityId;
    }

    public void setIdentityId(String identityId) {
        this.identityId = identityId;
    }

    public String getWithdrawalId() {
        return this.withdrawalId;
    }

    public void setWithdrawalId(String withdrawalId) {
        this.withdrawalId = withdrawalId;
    }

    public String getWalletId() {
        return this.walletId;
    }

    public void setWalletId(String walletId) {
        this.walletId = walletId;
    }

    public String getDestinationId() {
        return this.destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    public Long getAmount() {
        return this.amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public String getCurrencyCode() {
        return this.currencyCode;
    }

    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final WithdrawalData other = (WithdrawalData) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        }
        else if (!id.equals(other.id))
            return false;
        if (partyId == null) {
            if (other.partyId != null)
                return false;
        }
        else if (!partyId.equals(other.partyId))
            return false;
        if (identityId == null) {
            if (other.identityId != null)
                return false;
        }
        else if (!identityId.equals(other.identityId))
            return false;
        if (withdrawalId == null) {
            if (other.withdrawalId != null)
                return false;
        }
        else if (!withdrawalId.equals(other.withdrawalId))
            return false;
        if (walletId == null) {
            if (other.walletId != null)
                return false;
        }
        else if (!walletId.equals(other.walletId))
            return false;
        if (destinationId == null) {
            if (other.destinationId != null)
                return false;
        }
        else if (!destinationId.equals(other.destinationId))
            return false;
        if (amount == null) {
            if (other.amount != null)
                return false;
        }
        else if (!amount.equals(other.amount))
            return false;
        if (currencyCode == null) {
            if (other.currencyCode != null)
                return false;
        }
        else if (!currencyCode.equals(other.currencyCode))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.id == null) ? 0 : this.id.hashCode());
        result = prime * result + ((this.partyId == null) ? 0 : this.partyId.hashCode());
        result = prime * result + ((this.identityId == null) ? 0 : this.identityId.hashCode());
        result = prime * result + ((this.withdrawalId == null) ? 0 : this.withdrawalId.hashCode());
        result = prime * result + ((this.walletId == null) ? 0 : this.walletId.hashCode());
        result = prime * result + ((this.destinationId == null) ? 0 : this.destinationId.hashCode());
        result = prime * result + ((this.amount == null) ? 0 : this.amount.hashCode());
        result = prime * result + ((this.currencyCode == null) ? 0 : this.currencyCode.hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("WithdrawalData (");

        sb.append(id);
        sb.append(", ").append(partyId);
        sb.append(", ").append(identityId);
        sb.append(", ").append(withdrawalId);
        sb.append(", ").append(walletId);
        sb.append(", ").append(destinationId);
        sb.append(", ").append(amount);
        sb.append(", ").append(currencyCode);

        sb.append(")");
        return sb.toString();
    }
}
