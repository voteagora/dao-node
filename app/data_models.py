from collections import defaultdict
from .abcs import DataModel

class ParticipationRateModel(DataModel):
    def __init__(self):
        self.completed_participation_fractions = defaultdict(lambda : (0, 0))
        self.future_participation_fractions = defaultdict(int)

    def refresh_all_completed_participation_fractions(self, proposals_dp, votes_dp, delegations_dp):
        
        new_fractions = defaultdict(lambda : (0, 0))
        
        # This should be a loop of no more than 10...
        for proposal_id, start_block, _ in proposals_dp.prst.recently_completed_and_counted_proposals:

            # this is a giant loop, but ~200K for Optimism...
            for delegatee_addr in delegations_dp.delegatee_vp_history.keys():

                # this is a bisect algo 
                vp = delegations_dp.delegatee_vp_at_block(delegatee_addr, start_block)

                if vp > 0:
                        
                    voted = votes_dp.participated.get(delegatee_addr, {}).get(proposal_id, False)
                        
                    num, den = new_fractions[delegatee_addr]
                        
                    if voted:
                        num += 1
                        
                    den += 1
                        
                    new_fractions[delegatee_addr] = (num, den)

        self.completed_participation_fractions = new_fractions
   

    def refresh_all_future_participation_fractions(self, proposals_dp, votes_dp, delegations_dp):
        
        new_fractions = defaultdict(int)
        
        # This should be a loop of no more than 10...
        for proposal_id, start_block, end_block in proposals_dp.prst.ending_in_future_proposals:

            # this is a giant loop, but ~200K for Optimism...
            for delegatee_addr in delegations_dp.delegatee_vp_history.keys():

                num = new_fractions[delegatee_addr]

                # this is a bisect algo 
                vp = delegations_dp.delegatee_vp_at_block(delegatee_addr, start_block)

                if vp > 0:
                        
                    voted = votes_dp.participated.get(delegatee_addr, {}).get(proposal_id, False)
                    
                    if voted:
                        num += 1

                new_fractions[delegatee_addr] = num
                                
        self.future_participation_fractions = new_fractions


    def refresh_if_necessary(self, proposal_dp, votes_dp, delegations_dp):
        print(f"{proposal_dp.prst.flag_recently_completed_and_counted_has_changed=}, {proposal_dp.prst.flag_ending_in_future_proposals_has_changed=}")

        if proposal_dp.prst.flag_recently_completed_and_counted_has_changed:
            self.refresh_all_completed_participation_fractions(proposal_dp, votes_dp, delegations_dp)
            proposal_dp.prst.flag_recently_completed_and_counted_has_changed = False
        
        if proposal_dp.prst.flag_ending_in_future_proposals_has_changed:
            self.refresh_all_future_participation_fractions(proposal_dp, votes_dp, delegations_dp)
            proposal_dp.prst.flag_ending_in_future_proposals_has_changed = False

    def get_rate(self, delegatee_addr):
        
        cnum, cden = self.completed_participation_fractions[delegatee_addr]
        fnum       = self.future_participation_fractions[delegatee_addr]

        den = cden + fnum

        if den == 0:
            return 0.0
        
        num = cnum + fnum

        return num / den
    
    def get_fraction(self, delegatee_addr):
        
        cnum, cden = self.completed_participation_fractions[delegatee_addr]
        fnum       = self.future_participation_fractions[delegatee_addr]

        return (cnum + fnum), (cden + fnum)

    def rates(self):
        for addr in self.completed_participation_fractions.keys():
            yield addr, self.get_rate(addr)