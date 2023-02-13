package dev.vality.fistful.magista.dao;

import dev.vality.fistful.magista.domain.tables.pojos.ChallengeData;
import dev.vality.fistful.magista.domain.tables.pojos.IdentityData;
import dev.vality.fistful.magista.exception.DaoException;

public interface IdentityDao extends FistfulDao<IdentityData> {

    ChallengeData get(String identityId, String challengeId) throws DaoException;

    Long save(ChallengeData challenge) throws DaoException;

}
