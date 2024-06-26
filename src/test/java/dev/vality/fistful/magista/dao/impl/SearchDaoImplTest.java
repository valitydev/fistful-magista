package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.base.Failure;
import dev.vality.fistful.fistful_stat.RevertStatus;
import dev.vality.fistful.fistful_stat.StatDeposit;
import dev.vality.fistful.fistful_stat.StatWallet;
import dev.vality.fistful.fistful_stat.StatWithdrawal;
import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.*;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.fistful.magista.domain.enums.WithdrawalStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.query.impl.WalletFunction;
import dev.vality.fistful.magista.query.impl.WithdrawalFunction;
import dev.vality.fistful.magista.query.impl.parameters.DepositParameters;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import java.util.*;

import static dev.vality.fistful.magista.query.impl.Parameters.*;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SearchDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private SearchDao searchDao;

    @Autowired
    private WalletDao walletDao;

    @Autowired
    private WithdrawalDao withdrawalDao;

    @Autowired
    private DepositDao depositDao;

    @Autowired
    private DepositRevertDao depositRevertDao;

    @Test
    public void testGetWallets() throws DaoException {
        WalletData walletData = random(WalletData.class);
        walletDao.save(walletData);
        HashMap<String, Object> map = new HashMap<>();
        map.put(PARTY_ID_PARAM, walletData.getPartyId());
        map.put(IDENTITY_ID_PARAM, walletData.getIdentityId());
        map.put(CURRENCY_CODE_PARAM, walletData.getCurrencyCode());
        WalletFunction.WalletParameters walletParameters = new WalletFunction.WalletParameters(map, null);
        Collection<Map.Entry<Long, StatWallet>> wallets = searchDao.getWallets(
                walletParameters,
                Optional.empty(),
                100);
        assertEquals(1, wallets.size());
        assertEquals(wallets.iterator().next().getValue().getName(), walletData.getWalletName());

        map.clear();
        map.put(PARTY_ID_PARAM, UUID.randomUUID());
        walletParameters = new WalletFunction.WalletParameters(map, null);
        wallets = searchDao.getWallets(walletParameters, Optional.of(walletData.getCreatedAt()), 100);
        assertEquals(0, wallets.size());
    }

    @Test
    public void testGetWithdrawals() throws DaoException {
        WithdrawalData withdrawalData = random(WithdrawalData.class);
        withdrawalData.setWithdrawalStatus(WithdrawalStatus.failed);
        withdrawalData.setErrorCode("authorization_failed");
        withdrawalData.setErrorReason(null);
        withdrawalData.setErrorSubFailure("unknown");
        withdrawalDao.save(withdrawalData);

        HashMap<String, Object> map = new HashMap<>();
        map.put(PARTY_ID_PARAM, withdrawalData.getPartyId());
        map.put(WALLET_ID_PARAM, withdrawalData.getWalletId());
        map.put(IDENTITY_ID_PARAM, withdrawalData.getIdentityId());
        map.put(DESTINATION_ID_PARAM, withdrawalData.getDestinationId());
        map.put(EXTERNAL_ID_PARAM, withdrawalData.getExternalId());
        map.put(STATUS_PARAM, withdrawalData.getWithdrawalStatus().getLiteral());
        map.put(AMOUNT_FROM_PARAM, withdrawalData.getAmount() - 1);
        map.put(AMOUNT_TO_PARAM, withdrawalData.getAmount() + 1);
        map.put(CURRENCY_CODE_PARAM, withdrawalData.getCurrencyCode());
        map.put(WITHDRAWAL_PROVIDER_ID_PARAM, withdrawalData.getProviderId());
        map.put(WITHDRAWAL_TERMINAL_ID_PARAM, withdrawalData.getTerminalId());
        WithdrawalFunction.WithdrawalParameters withdrawalParameters =
                new WithdrawalFunction.WithdrawalParameters(map, null);
        Collection<Map.Entry<Long, StatWithdrawal>> withdrawals = searchDao.getWithdrawals(
                withdrawalParameters,
                withdrawalData.getCreatedAt().minusMinutes(1),
                withdrawalData.getCreatedAt().plusMinutes(1),
                withdrawalData.getId() + 1,
                100
        );
        assertEquals(1, withdrawals.size());
        StatWithdrawal statWithdrawal = withdrawals.iterator().next().getValue();
        assertEquals(statWithdrawal.getFee(), withdrawalData.getFee().longValue());

        assertTrue(statWithdrawal.getStatus().isSetFailed());
        Failure baseFailure = statWithdrawal.getStatus().getFailed().getBaseFailure();
        assertEquals(withdrawalData.getErrorCode(), baseFailure.getCode());
        assertEquals(withdrawalData.getErrorReason(), baseFailure.getReason());
        assertEquals(withdrawalData.getErrorSubFailure(), baseFailure.getSub().getCode());

        map.clear();
        map.put(IDENTITY_ID_PARAM, "wrong_identity_id");
        withdrawalParameters = new WithdrawalFunction.WithdrawalParameters(map, null);
        withdrawals = searchDao.getWithdrawals(
                withdrawalParameters,
                withdrawalData.getEventCreatedAt().minusMinutes(1),
                withdrawalData.getEventCreatedAt().plusMinutes(1),
                withdrawalData.getId() + 1,
                100
        );
        assertEquals(0, withdrawals.size());
    }

    @Test
    public void testGetWithdrawalsWithErrors() throws DaoException {
        WithdrawalData withdrawalData = random(WithdrawalData.class);
        withdrawalData.setWithdrawalStatus(WithdrawalStatus.failed);
        withdrawalData.setErrorCode("authorization_failed");
        withdrawalData.setErrorReason(null);
        withdrawalData.setErrorSubFailure("unknown");
        withdrawalDao.save(withdrawalData);

        HashMap<String, Object> map = new HashMap<>();
        map.put(ERROR_MESSAGE, "unknown");
        WithdrawalFunction.WithdrawalParameters withdrawalParameters =
                new WithdrawalFunction.WithdrawalParameters(map, null);
        Collection<Map.Entry<Long, StatWithdrawal>> withdrawals = searchDao.getWithdrawals(
                withdrawalParameters,
                withdrawalData.getCreatedAt().minusMinutes(1),
                withdrawalData.getCreatedAt().plusMinutes(1),
                withdrawalData.getId() + 1,
                100
        );
        assertEquals(1, withdrawals.size());
        StatWithdrawal statWithdrawal = withdrawals.iterator().next().getValue();
        assertEquals(statWithdrawal.getFee(), withdrawalData.getFee().longValue());
        assertTrue(statWithdrawal.getStatus().isSetFailed());


        map = new HashMap<>();
        map.put(ERROR_MESSAGE, "test_for_empty");
        withdrawalParameters = new WithdrawalFunction.WithdrawalParameters(map, null);
        withdrawals = searchDao.getWithdrawals(
                withdrawalParameters,
                withdrawalData.getCreatedAt().minusMinutes(1),
                withdrawalData.getCreatedAt().plusMinutes(1),
                withdrawalData.getId() + 1,
                100
        );

        assertTrue(withdrawals.isEmpty());
    }

    @Test
    public void testGetWithdrawalsByProviderId() throws DaoException {
        WithdrawalData withdrawalData = random(WithdrawalData.class);
        withdrawalData.setWithdrawalStatus(WithdrawalStatus.succeeded);
        withdrawalDao.save(withdrawalData);

        HashMap<String, Object> map = new HashMap<>();
        map.put(WITHDRAWAL_PROVIDER_ID_PARAM, withdrawalData.getProviderId());
        map.put(WITHDRAWAL_TERMINAL_ID_PARAM, withdrawalData.getTerminalId());
        WithdrawalFunction.WithdrawalParameters withdrawalParameters =
                new WithdrawalFunction.WithdrawalParameters(map, null);
        Collection<Map.Entry<Long, StatWithdrawal>> withdrawals = searchDao.getWithdrawals(
                withdrawalParameters,
                withdrawalData.getCreatedAt().minusMinutes(1),
                withdrawalData.getCreatedAt().plusMinutes(1),
                withdrawalData.getId() + 1,
                25
        );
        assertEquals(1, withdrawals.size());
        StatWithdrawal statWithdrawal = withdrawals.iterator().next().getValue();
        assertEquals(withdrawalData.getFee().longValue(), statWithdrawal.getFee());
        assertTrue(statWithdrawal.getStatus().isSetSucceeded());
        assertEquals(withdrawalData.getProviderId().intValue(), statWithdrawal.getProviderId());
        assertEquals(withdrawalData.getTerminalId().intValue(), statWithdrawal.getTerminalId());
    }

    @Test
    public void testGetDeposits() throws DaoException {
        DepositData deposit = random(DepositData.class);
        depositDao.save(deposit);

        HashMap<String, Object> map = buildDepositSearchMap(deposit);

        Collection<Map.Entry<Long, StatDeposit>> deposits = getDeposits(deposit, new DepositParameters(map, null));
        assertEquals(1, deposits.size());
        StatDeposit statDeposit = deposits.iterator().next().getValue();
        assertEquals(statDeposit.getFee(), deposit.getFee().longValue());
        assertEquals(RevertStatus.none, statDeposit.getRevertStatus());

        map.clear();
        map.put(IDENTITY_ID_PARAM, "wrong_identity_id");
        map.put(PARTY_ID_PARAM, deposit.getPartyId());
        deposits = getDeposits(deposit, new DepositParameters(map, null));
        assertEquals(0, deposits.size());
    }

    @Test
    public void testRevertStatusFullDeposits() throws DaoException {
        DepositData depositOne = random(DepositData.class);
        depositDao.save(depositOne);

        DepositRevertData depositRevertDataOne = random(DepositRevertData.class);
        depositRevertDataOne.setStatus(DepositRevertDataStatus.succeeded);
        depositRevertDataOne.setPartyId(depositOne.getPartyId());
        depositRevertDataOne.setWalletId(depositOne.getWalletId());
        depositRevertDataOne.setDepositId(depositOne.getDepositId());
        depositRevertDataOne.setAmount(depositOne.getAmount());

        depositRevertDao.save(depositRevertDataOne);
        HashMap<String, Object> map = buildDepositSearchMap(depositOne);
        Collection<Map.Entry<Long, StatDeposit>> deposits = getDeposits(depositOne, new DepositParameters(map, null));
        assertEquals(1, deposits.size());
        assertEquals(RevertStatus.full, deposits.iterator().next().getValue().getRevertStatus());
    }

    @Test
    public void testRevertStatusNoneDeposits() throws DaoException {
        DepositData depositOne = random(DepositData.class);
        depositDao.save(depositOne);

        DepositRevertData depositRevertDataOne = random(DepositRevertData.class);
        depositRevertDataOne.setStatus(DepositRevertDataStatus.pending);
        depositRevertDataOne.setPartyId(depositOne.getPartyId());
        depositRevertDataOne.setWalletId(depositOne.getWalletId());
        depositRevertDataOne.setDepositId(depositOne.getDepositId());
        depositRevertDataOne.setAmount(depositOne.getAmount());

        depositRevertDao.save(depositRevertDataOne);
        HashMap<String, Object> map = buildDepositSearchMap(depositOne);
        Collection<Map.Entry<Long, StatDeposit>> deposits = getDeposits(depositOne, new DepositParameters(map, null));
        assertEquals(1, deposits.size());
        assertEquals(RevertStatus.none, deposits.iterator().next().getValue().getRevertStatus());
    }

    @Test
    public void testRevertStatusPartialDeposits() throws DaoException {
        DepositData depositOne = random(DepositData.class);
        depositOne.setAmount(100L);
        depositDao.save(depositOne);

        DepositRevertData depositRevertDataOne = random(DepositRevertData.class);
        depositRevertDataOne.setStatus(DepositRevertDataStatus.succeeded);
        depositRevertDataOne.setPartyId(depositOne.getPartyId());
        depositRevertDataOne.setWalletId(depositOne.getWalletId());
        depositRevertDataOne.setDepositId(depositOne.getDepositId());
        depositRevertDataOne.setAmount(50L);

        DepositRevertData depositRevertDataTwo = random(DepositRevertData.class);
        depositRevertDataTwo.setStatus(DepositRevertDataStatus.succeeded);
        depositRevertDataTwo.setPartyId(depositOne.getPartyId());
        depositRevertDataTwo.setWalletId(depositOne.getWalletId());
        depositRevertDataTwo.setDepositId(depositOne.getDepositId());
        depositRevertDataTwo.setAmount(40L);

        depositRevertDao.save(depositRevertDataOne);
        depositRevertDao.save(depositRevertDataTwo);

        HashMap<String, Object> map = buildDepositSearchMap(depositOne);
        Collection<Map.Entry<Long, StatDeposit>> deposits = getDeposits(depositOne, new DepositParameters(map, null));
        assertEquals(1, deposits.size());
        assertEquals(RevertStatus.partial, deposits.iterator().next().getValue().getRevertStatus());
    }

    private Collection<Map.Entry<Long, StatDeposit>> getDeposits(DepositData deposit, DepositParameters parameters)
            throws DaoException {
        return searchDao.getDeposits(
                parameters,
                null,
                null,
                deposit.getId() + 1,
                100
        );
    }

    private HashMap<String, Object> buildDepositSearchMap(DepositData deposit) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(IDENTITY_ID_PARAM, deposit.getIdentityId());
        map.put(WALLET_ID_PARAM, deposit.getWalletId());
        map.put(SOURCE_ID_PARAM, deposit.getSourceId());
        map.put(PARTY_ID_PARAM, deposit.getPartyId());
        map.put(AMOUNT_FROM_PARAM, deposit.getAmount() - 1);
        map.put(AMOUNT_TO_PARAM, deposit.getAmount() + 1);
        map.put(CURRENCY_CODE_PARAM, deposit.getCurrencyCode());
        map.put(STATUS_PARAM, StringUtils.capitalize(deposit.getDepositStatus().getLiteral()));
        return map;
    }
}
