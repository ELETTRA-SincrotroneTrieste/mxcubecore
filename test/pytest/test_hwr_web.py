from HardwareRepository import HardwareRepository

def test_hwr_web(hwr_web):
    hwr = HardwareRepository.getHardwareRepository(hwr_web)
    hwr.connect()
    blsetup_hwobj = hwr.getHardwareObject("beamline-setup")
